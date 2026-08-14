import type { Meta, StoryObj } from "@storybook/react-vite";
import { SAMPLE_CHANNEL_ID } from "@/mocks/channels-mocks";
import {
    iframeHandlers,
    iframeMocks,
    SAMPLE_CHANNEL_THEME,
    SAMPLE_DEFAULT_THEME,
    SAMPLE_FONT_OPTIONS,
} from "@/mocks/iframe-mocks";
import { AppWebWidgetCustomize } from "./app-web-widget-customize";

const meta = {
    title: "Apps/Web widget appearance",
    component: AppWebWidgetCustomize,
    parameters: {
        page: { title: "Web widget appearance" },
        router: {
            initialPath: `/apps/demo/web-widget/${SAMPLE_CHANNEL_ID}/customize`,
            path: "/apps/:slug/web-widget/:channelId/customize",
        },
    },
} satisfies Meta<typeof AppWebWidgetCustomize>;

export default meta;

type Story = StoryObj<typeof meta>;

// The widget has a theme of its own: the form shows it and "Follow app default" is offered.
export const Default: Story = {};

// The widget follows the app default (no theme of its own): the form is seeded from the default and says so.
export const FollowsAppDefault: Story = {
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getTheme({
                        theme: null,
                        defaultTheme: SAMPLE_DEFAULT_THEME,
                        fontOptions: SAMPLE_FONT_OPTIONS,
                    }),
                    ...iframeHandlers(),
                ],
            },
        },
    },
};

// A dark, high-radius theme: the preview renders the widget's dark palette derived from the same accent.
export const DarkTheme: Story = {
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getTheme({
                        theme: {
                            ...SAMPLE_CHANNEL_THEME,
                            appearance: "Dark",
                            dark: { buttonColor: "#1d4ed8", messageColor: "#16233f", backgroundColor: "#0d1117" },
                            radius: "Large",
                        },
                        defaultTheme: SAMPLE_DEFAULT_THEME,
                        fontOptions: SAMPLE_FONT_OPTIONS,
                    }),
                    ...iframeHandlers(),
                ],
            },
        },
    },
};

// Nothing optional set: no header, no greeting, no prompts, no disclaimer — the leanest widget an operator
// can configure, and the one most likely to expose a missing empty-state fallback.
export const MinimalTheme: Story = {
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getTheme({
                        theme: {
                            ...SAMPLE_CHANNEL_THEME,
                            showHeader: false,
                            headerSubtitle: null,
                            greetingTitle: null,
                            greetingBody: null,
                            suggestedPrompts: [],
                            disclaimer: null,
                        },
                        defaultTheme: SAMPLE_DEFAULT_THEME,
                        fontOptions: SAMPLE_FONT_OPTIONS,
                    }),
                    ...iframeHandlers(),
                ],
            },
        },
    },
};

// The channelId in the URL isn't a web widget in this app — the page shows a not-found alert.
export const UnknownWidget: Story = {
    parameters: {
        router: {
            initialPath: "/apps/demo/web-widget/unknown/customize",
            path: "/apps/:slug/web-widget/:channelId/customize",
        },
    },
};
