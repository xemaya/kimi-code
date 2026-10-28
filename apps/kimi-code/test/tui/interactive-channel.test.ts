import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { createConnection, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';

import type { Event, Session } from '@moonshot-ai/kimi-code-sdk';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { InteractiveChannelController } from '#/tui/controllers/interactive-channel';
import { isSteerableQueuedMessage } from '#/tui/kimi-tui';

interface ChannelMessage extends Record<string, unknown> {
  type: string;
  request_id?: string;
  session_id?: string;
  turn_id?: number;
  delta?: string;
  status?: string;
  output_gap?: boolean;
}

const roots: string[] = [];
const controllers: InteractiveChannelController[] = [];
const clients: Socket[] = [];

afterEach(async () => {
  for (const client of clients.splice(0)) client.destroy();
  for (const controller of controllers.splice(0)) await controller.close();
  for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

function fakeSession(id: string) {
  const listeners = new Set<(event: Event) => void>();
  const session = {
    id,
    onEvent(listener: (event: Event) => void) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  } as unknown as Session;
  return {
    session,
    emit(event: Event) {
      for (const listener of listeners) listener(event);
    },
  };
}

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'kimi-channel-'));
  roots.push(root);
  const socketPath = join(root, 'channel.sock');
  const inputs: Array<{ text: string; promptId: string }> = [];
  const errors: string[] = [];
  const controller = new InteractiveChannelController({
    sendChannelInput: vi.fn(async (text: string, promptId: string) => {
      inputs.push({ text, promptId });
    }),
    showError: (message) => errors.push(message),
  }, socketPath);
  controllers.push(controller);
  await controller.start();
  return { controller, socketPath, inputs, errors };
}

async function connect(socketPath: string) {
  const socket = createConnection(socketPath);
  clients.push(socket);
  const messages: ChannelMessage[] = [];
  createInterface({ input: socket }).on('line', (line) => {
    messages.push(JSON.parse(line) as ChannelMessage);
  });
  await new Promise<void>((resolve, reject) => {
    socket.once('connect', resolve);
    socket.once('error', reject);
  });
  return { socket, messages };
}

function write(socket: Socket, value: unknown): void {
  socket.write(`${JSON.stringify(value)}\n`);
}

describe('InteractiveChannelController', () => {
  it('correlates one external prompt with only its native turn', async () => {
    const { controller, socketPath, inputs, errors } = await fixture();
    const { session, emit } = fakeSession('ses_channel');
    controller.attach(session);
    const { socket, messages } = await connect(socketPath);
    expect(messages).toHaveLength(0);
    controller.markReady(session);
    await vi.waitFor(() => {
      expect(messages.map((message) => message.type)).toContain('ready');
    });
    expect((await stat(socketPath)).mode & 0o777).toBe(0o600);

    write(socket, { version: 1, type: 'prompt', id: 'cmd_external', text: '/ordinary-text' });
    await vi.waitFor(() => {
      expect(inputs).toHaveLength(1);
    });
    expect(inputs[0]!.text).toBe('/ordinary-text');
    const enginePromptId = inputs[0]!.promptId;
    expect(enginePromptId).not.toBe('cmd_external');
    await vi.waitFor(() => {
      expect(messages.map((message) => message.type)).toContain('prompt.accepted');
    });
    emit({ type: 'turn.started', promptId: enginePromptId, turnId: 7 } as Event);
    emit({ type: 'assistant.delta', turnId: 8, delta: 'wrong-turn' } as Event);
    emit({ type: 'assistant.delta', turnId: 7, delta: 'wrong-agent', agentId: 'child-agent' } as Event);
    emit({ type: 'turn.ended', turnId: 7, reason: 'failed', agentId: 'child-agent' } as Event);
    expect(messages.filter((message) => message.type === 'prompt.completed')).toHaveLength(0);
    emit({ type: 'assistant.delta', turnId: 7, delta: 'native-result' } as Event);
    emit({ type: 'turn.step.completed', turnId: 7, finishReason: 'max_tokens' } as Event);
    emit({ type: 'turn.ended', turnId: 7, reason: 'completed' } as Event);

    await vi.waitFor(() => {
      expect(messages.map((message) => message.type)).toContain('prompt.completed');
    });
    const completed = messages.find((message) => message.type === 'prompt.completed');
    expect(completed).toMatchObject({ request_id: 'cmd_external', turn_id: 7, status: 'succeeded', output_gap: true });
    expect(completed).not.toHaveProperty('error');
    expect(messages.filter((message) => message.type === 'assistant.delta')).toHaveLength(1);
    expect(messages.find((message) => message.type === 'assistant.delta')).toMatchObject({ delta: 'native-result', turn_id: 7 });
    write(socket, { version: 1, type: 'prompt', id: 'cmd_external', text: 'must-not-repeat' });
    await vi.waitFor(() => {
      expect(messages).toContainEqual(expect.objectContaining({ type: 'error', request_id: 'cmd_external', error: 'Duplicate request id' }));
    });
    expect(inputs).toHaveLength(1);
    expect(errors).toEqual([]);
  });

  it('rejects malformed and concurrent input and detaches the old Session', async () => {
    const { controller, socketPath, inputs } = await fixture();
    const first = fakeSession('ses_old');
    controller.attach(first.session);
    controller.markReady(first.session);
    const { socket, messages } = await connect(socketPath);
    await vi.waitFor(() => {
      expect(messages).toHaveLength(1);
    });
    socket.write('not-json\n');
    await vi.waitFor(() => {
      expect(messages.map((message) => message.type)).toContain('error');
    });

    write(socket, { version: 1, type: 'prompt', id: 'cmd_first', text: 'first' });
    await vi.waitFor(() => {
      expect(inputs).toHaveLength(1);
    });
    write(socket, { version: 1, type: 'prompt', id: 'cmd_second', text: 'second' });
    await vi.waitFor(() => {
      expect(messages.some((message) => message.type === 'error' && message.request_id === 'cmd_second')).toBe(true);
    });
    expect(inputs).toHaveLength(1);

    controller.detach(first.session);
    await vi.waitFor(() => {
      expect(messages.some((message) => message.type === 'prompt.completed' && message.status === 'failed')).toBe(true);
    });
    const second = fakeSession('ses_new');
    controller.attach(second.session);
    controller.markReady(second.session);
    await vi.waitFor(() => {
      expect(messages.some((message) => message.type === 'ready' && message.session_id === 'ses_new')).toBe(true);
    });
    first.emit({ type: 'assistant.delta', turnId: 1, delta: 'must-not-leak' } as Event);
    expect(messages.filter((message) => message.type === 'assistant.delta')).toHaveLength(0);
  });

  it('settles blocked prompts and dispatch failures without a turn', async () => {
    const { controller, socketPath, inputs } = await fixture();
    const { session, emit } = fakeSession('ses_hooks');
    controller.attach(session);
    controller.markReady(session);
    const { socket, messages } = await connect(socketPath);
    write(socket, { version: 1, type: 'prompt', id: 'cmd_blocked', text: 'blocked by a hook' });
    await vi.waitFor(() => {
      expect(inputs).toHaveLength(1);
    });
    emit({ type: 'prompt.completed', promptId: 'another-prompt', reason: 'blocked' } as Event);
    expect(messages.filter((message) => message.type === 'prompt.completed')).toHaveLength(0);
    emit({ type: 'prompt.completed', promptId: inputs[0]!.promptId, reason: 'blocked' } as Event);
    await vi.waitFor(() => {
      expect(messages.some((message) => message.type === 'prompt.completed' && message.status === 'failed')).toBe(true);
    });

    write(socket, { version: 1, type: 'prompt', id: 'cmd_dispatch', text: 'cannot dispatch' });
    await vi.waitFor(() => {
      expect(inputs).toHaveLength(2);
    });
    controller.handleDispatchError(inputs[1]!.promptId, new Error('dispatch failed'));
    await vi.waitFor(() => {
      expect(messages.filter((message) => message.type === 'prompt.completed')).toHaveLength(2);
    });
    expect(messages.filter((message) => message.type === 'prompt.started')).toHaveLength(0);
  });

  it('flushes all queued output after socket backpressure', async () => {
    const { controller, socketPath, inputs } = await fixture();
    const { session, emit } = fakeSession('ses_backpressure');
    controller.attach(session);
    controller.markReady(session);
    const { socket, messages } = await connect(socketPath);
    await vi.waitFor(() => {
      expect(messages.some((message) => message.type === 'ready')).toBe(true);
    });
    write(socket, { version: 1, type: 'prompt', id: 'cmd_backpressure', text: 'stream' });
    await vi.waitFor(() => {
      expect(inputs).toHaveLength(1);
    });
    emit({ type: 'turn.started', promptId: inputs[0]!.promptId, turnId: 9 } as Event);
    socket.pause();
    emit({ type: 'assistant.delta', turnId: 9, delta: 'x'.repeat(4 * 1024 * 1024) } as Event);
    emit({ type: 'assistant.delta', turnId: 9, delta: 'after-backpressure' } as Event);
    emit({ type: 'turn.ended', turnId: 9, reason: 'completed' } as Event);
    socket.resume();
    await vi.waitFor(() => {
      expect(messages.some((message) => message.type === 'prompt.completed')).toBe(true);
    });
    expect(messages.filter((message) => message.type === 'assistant.delta')).toHaveLength(2);
  });

  it('does not steer external prompts into another turn', () => {
    expect(isSteerableQueuedMessage({ text: 'external', promptId: 'channel-request' })).toBe(false);
    expect(isSteerableQueuedMessage({ text: 'ordinary' })).toBe(true);
  });

  it('does not replace a live socket or a non-socket path', async () => {
    const first = await fixture();
    const second = new InteractiveChannelController({ sendChannelInput: vi.fn(), showError: vi.fn() }, first.socketPath);
    controllers.push(second);
    await expect(second.start()).rejects.toThrow('already active');
    await second.close();
    const { socket, messages } = await connect(first.socketPath);
    socket.write('not-json\n');
    await vi.waitFor(() => {
      expect(messages.some((message) => message.type === 'error')).toBe(true);
    });

    const root = await mkdtemp(join(tmpdir(), 'kimi-channel-'));
    roots.push(root);
    const socketPath = join(root, 'plain-file');
    const controller = new InteractiveChannelController({ sendChannelInput: vi.fn(), showError: vi.fn() }, socketPath);
    controllers.push(controller);
    await writeFile(socketPath, 'do-not-overwrite');
    await expect(controller.start()).rejects.toThrow('not a socket');
    expect(await readFile(socketPath, 'utf8')).toBe('do-not-overwrite');
  });
});
