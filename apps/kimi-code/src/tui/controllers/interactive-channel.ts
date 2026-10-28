import { chmodSync } from 'node:fs';
import { lstat, mkdir, unlink } from 'node:fs/promises';
import { createConnection, createServer, type Server, type Socket } from 'node:net';

import type { Event, Session, Unsubscribe } from '@moonshot-ai/kimi-code-sdk';
import { dirname, resolve } from 'pathe';

import {
  INTERACTIVE_CHANNEL_MAX_FRAME_BYTES,
  INTERACTIVE_CHANNEL_MAX_PROMPT_LENGTH,
  INTERACTIVE_CHANNEL_MAX_REQUEST_ID_LENGTH,
  INTERACTIVE_CHANNEL_VERSION,
} from '../constant/interactive-channel';
import { MAIN_AGENT_ID } from '../constant/kimi-tui';

interface InteractiveChannelHost {
  sendChannelInput(text: string, promptId: string): Promise<void>;
  showError(message: string): void;
}

interface ActivePrompt {
  readonly id: string;
  readonly promptId: string;
  turnId?: number;
  text: string;
  outputGap: boolean;
}

export class InteractiveChannelController {
  private readonly socketPath: string;
  private server?: Server;
  private client?: Socket;
  private readonly connections = new Set<Socket>();
  private session?: Session;
  private sessionUnsubscribe?: Unsubscribe;
  private readySessionId?: string;
  private readyClient?: Socket;
  private active?: ActivePrompt;
  private readonly requestIds = new Set<string>();
  private readonly outputQueue: Array<Record<string, unknown>> = [];
  private outputPending = false;
  private closed = false;

  constructor(private readonly host: InteractiveChannelHost, socketPath: string) {
    this.socketPath = resolve(socketPath);
  }

  async start(): Promise<void> {
    if (this.server !== undefined || this.closed) return;
    await mkdir(dirname(this.socketPath), { recursive: true, mode: 0o700 });
    await this.removeStaleSocket();
    const server = createServer((socket) => {
      this.accept(socket);
    });
    this.server = server;
    try {
      await new Promise<void>((resolve, reject) => {
        server.once('error', reject);
        server.listen(this.socketPath, () => {
          server.off('error', reject);
          resolve();
        });
      });
      chmodSync(this.socketPath, 0o600);
    } catch (error) {
      this.server = undefined;
      await new Promise<void>((resolve) => {
        server.close(() => {
          resolve();
        });
      });
      throw error;
    }
    server.on('error', (error) => {
      this.host.showError(`Interactive channel failed: ${error.message}`);
    });
    this.sendReady();
  }

  attach(session: Session): void {
    if (this.closed || this.session?.id === session.id) return;
    this.detach(this.session);
    this.session = session;
    this.sessionUnsubscribe = session.onEvent((event) => {
      this.handleEvent(event);
    });
  }

  markReady(session: Session): void {
    if (this.closed || this.session?.id !== session.id || this.readySessionId === session.id) return;
    this.readySessionId = session.id;
    this.sendReady();
  }

  private sendReady(): void {
    if (this.client === undefined || this.readySessionId === undefined || this.readyClient === this.client) return;
    this.readyClient = this.client;
    this.send({ version: INTERACTIVE_CHANNEL_VERSION, type: 'ready', session_id: this.readySessionId });
  }

  detach(session?: Session): void {
    if (session === undefined || this.session?.id === session.id) {
      this.sessionUnsubscribe?.();
      this.sessionUnsubscribe = undefined;
      this.session = undefined;
      this.readySessionId = undefined;
      this.readyClient = undefined;
      this.failActive('Session changed before the prompt completed');
    }
  }

  handleDispatchError(promptId: string | undefined, error: unknown): void {
    if (promptId === undefined || this.active?.promptId !== promptId) return;
    this.complete('failed', error instanceof Error ? error.message : String(error));
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.detach(this.session);
    for (const connection of this.connections) connection.destroy();
    this.connections.clear();
    this.client = undefined;
    this.readyClient = undefined;
    this.outputQueue.length = 0;
    this.outputPending = false;
    const server = this.server;
    this.server = undefined;
    if (server === undefined) return;
    await new Promise<void>((resolve) => {
      server.close(() => {
        resolve();
      });
    });
    try {
      const info = await lstat(this.socketPath);
      if (info.isSocket()) await unlink(this.socketPath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }

  private async removeStaleSocket(): Promise<void> {
    let isSocket = false;
    try {
      isSocket = (await lstat(this.socketPath)).isSocket();
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return;
      throw error;
    }
    if (!isSocket) throw new Error(`Interactive channel path is not a socket: ${this.socketPath}`);
    const active = await new Promise<boolean>((resolve, reject) => {
      const probe = createConnection(this.socketPath);
      const timer = setTimeout(() => probe.destroy(new Error('Interactive channel probe timed out')), 1000);
      probe.once('connect', () => {
        clearTimeout(timer);
        probe.destroy();
        resolve(true);
      });
      probe.once('error', (error: NodeJS.ErrnoException) => {
        clearTimeout(timer);
        if (error.code === 'ECONNREFUSED') {
          resolve(false);
          return;
        }
        reject(error);
      });
    });
    if (active) throw new Error(`Interactive channel is already active: ${this.socketPath}`);
    await unlink(this.socketPath);
  }

  private accept(socket: Socket): void {
    this.connections.add(socket);
    socket.once('close', () => {
      this.connections.delete(socket);
    });
    if (this.client !== undefined) {
      socket.end(`${JSON.stringify({ version: INTERACTIVE_CHANNEL_VERSION, type: 'error', error: 'Another client is already connected' })}\n`);
      return;
    }
    this.client = socket;
    socket.setEncoding('utf8');
    socket.on('error', () => {});
    socket.on('drain', () => {
      this.outputPending = false;
      this.flushOutput();
    });
    socket.on('close', () => {
      if (this.client !== socket) return;
      this.client = undefined;
      this.readyClient = undefined;
      this.active = undefined;
      this.outputQueue.length = 0;
      this.outputPending = false;
    });
    this.sendReady();
    let pending = '';
    socket.on('data', (chunk: string) => {
      pending += chunk;
      let newline = pending.indexOf('\n');
      while (newline !== -1) {
        const line = pending.slice(0, newline);
        pending = pending.slice(newline + 1);
        if (Buffer.byteLength(line, 'utf8') > INTERACTIVE_CHANNEL_MAX_FRAME_BYTES) {
          this.failRequest(undefined, 'Channel frame is too large');
          socket.end();
          return;
        }
        if (line.length > 0) {
          try {
            const message: unknown = JSON.parse(line);
            void this.handleRequest(message).catch((error) => {
              this.failRequest(undefined, error instanceof Error ? error.message : String(error));
            });
          } catch {
            this.failRequest(undefined, 'Invalid Channel JSON');
          }
        }
        newline = pending.indexOf('\n');
      }
      if (Buffer.byteLength(pending, 'utf8') > INTERACTIVE_CHANNEL_MAX_FRAME_BYTES) {
        this.failRequest(undefined, 'Channel frame is too large');
        socket.end();
      }
    });
  }

  private async handleRequest(message: unknown): Promise<void> {
    if (typeof message !== 'object' || message === null) {
      this.failRequest(undefined, 'Invalid Channel request');
      return;
    }
    const request = message as Record<string, unknown>;
    if (request['version'] !== INTERACTIVE_CHANNEL_VERSION || request['type'] !== 'prompt') {
      this.failRequest(undefined, 'Unsupported Channel request');
      return;
    }
    const requestId = request['id'];
    if (typeof requestId !== 'string' || requestId.length === 0 || requestId.length > INTERACTIVE_CHANNEL_MAX_REQUEST_ID_LENGTH) {
      this.failRequest(undefined, 'Invalid request id');
      return;
    }
    if (this.active !== undefined) {
      this.failRequest(requestId, 'Another external prompt is already active');
      return;
    }
    if (this.requestIds.has(requestId)) {
      this.failRequest(requestId, 'Duplicate request id');
      return;
    }
    const text = request['text'];
    if (typeof text !== 'string' || text.trim().length === 0 || text.length > INTERACTIVE_CHANNEL_MAX_PROMPT_LENGTH) {
      this.failRequest(requestId, 'Prompt must be non-empty text within the size limit');
      return;
    }
    if (this.session === undefined || this.readySessionId === undefined) {
      this.failRequest(requestId, 'No interactive Session is ready');
      return;
    }
    this.active = { id: requestId, promptId: `channel-${crypto.randomUUID()}-${requestId}`, text: '', outputGap: false };
    this.requestIds.add(requestId);
    if (this.requestIds.size > 128) {
      const oldest = this.requestIds.values().next().value;
      if (oldest !== undefined) this.requestIds.delete(oldest);
    }
    const promptId = this.active.promptId;
    try {
      await this.host.sendChannelInput(text, promptId);
      if (this.active?.id === requestId) this.send({ version: INTERACTIVE_CHANNEL_VERSION, type: 'prompt.accepted', request_id: requestId });
    } catch (error) {
      this.handleDispatchError(promptId, error);
    }
  }

  private handleEvent(event: Event): void {
    const active = this.active;
    if (active === undefined) return;
    if ('agentId' in event && event.agentId !== MAIN_AGENT_ID) return;
    if (event.type === 'prompt.completed' && event.reason === 'blocked' && event.promptId === active.promptId && active.turnId === undefined) {
      this.complete('failed', 'Prompt was blocked before the turn started');
      return;
    }
    if (event.type === 'turn.started' && event.promptId === active.promptId) {
      active.turnId = event.turnId;
      this.send({ version: INTERACTIVE_CHANNEL_VERSION, type: 'prompt.started', request_id: active.id, turn_id: event.turnId });
      return;
    }
    if (active.turnId === undefined || !('turnId' in event) || event.turnId !== active.turnId) return;
    if (event.type === 'assistant.delta') {
      active.text += event.delta;
      this.send({ version: INTERACTIVE_CHANNEL_VERSION, type: 'assistant.delta', request_id: active.id, turn_id: active.turnId, delta: event.delta });
      return;
    }
    if (event.type === 'turn.step.completed' && event.finishReason === 'max_tokens') active.outputGap = true;
    if (event.type === 'turn.step.interrupted' && ['max_steps', 'max_tokens'].includes(event.reason)) active.outputGap = true;
    if (event.type === 'turn.ended') {
      const status = event.reason === 'completed' ? 'succeeded' : event.reason === 'cancelled' ? 'cancelled' : 'failed';
      this.complete(status, event.error?.message ?? `Turn ended with ${event.reason}`);
    }
  }

  private complete(status: 'succeeded' | 'failed' | 'cancelled', error?: string): void {
    const active = this.active;
    if (active === undefined) return;
    this.active = undefined;
    this.send({
      version: INTERACTIVE_CHANNEL_VERSION,
      type: 'prompt.completed',
      request_id: active.id,
      turn_id: active.turnId,
      status,
      error: status === 'succeeded' ? undefined : error,
      output_gap: active.outputGap,
    });
  }

  private failActive(message: string): void {
    if (this.active !== undefined) this.complete('failed', message);
  }

  private failRequest(id: string | undefined, error: string): void {
    this.send({ version: INTERACTIVE_CHANNEL_VERSION, type: 'error', request_id: id, error });
  }

  private send(message: Record<string, unknown>): void {
    this.outputQueue.push(message);
    if (this.outputQueue.length > 256) {
      this.client?.destroy();
      return;
    }
    this.flushOutput();
  }

  private flushOutput(): void {
    while (!this.outputPending && this.client !== undefined && !this.client.destroyed) {
      const message = this.outputQueue.shift();
      if (message === undefined) return;
      this.outputPending = !this.client.write(`${JSON.stringify(message)}\n`);
    }
  }
}
