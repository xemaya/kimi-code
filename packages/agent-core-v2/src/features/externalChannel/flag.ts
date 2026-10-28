import { type FlagDefinitionInput, registerFlagDefinition } from '#/app/flag/flagRegistry';

export const TUI_CHANNEL_FLAG_ID = 'tui_channel';
export const TUI_CHANNEL_FLAG_ENV = 'KIMI_CODE_EXPERIMENTAL_TUI_CHANNEL';

export const tuiChannelFlag: FlagDefinitionInput = {
  id: TUI_CHANNEL_FLAG_ID,
  title: 'Interactive TUI channel',
  description: 'Allow a trusted local client to submit prompts to the native TUI over a Unix socket.',
  env: TUI_CHANNEL_FLAG_ENV,
  default: false,
  surface: 'tui',
};

registerFlagDefinition(tuiChannelFlag);
