import type { Meta, StoryObj } from "@storybook/react-vite";
import { iframeHandlers, iframeMocks, SAMPLE_CHANNEL_THEME, SAMPLE_FONT_OPTIONS } from "@/mocks/iframe-mocks";
import { AppWebWidgetDefaultCustomize } from "./app-web-widget-default-customize";

const meta = {
    title: "Apps/Default web widget appearance",
    component: AppWebWidgetDefaultCustomize,
    parameters: {
        page: { title: "Default web widget appearance" },
        router: {
            initialPath: "/apps/demo/web-widget/default-customize",
            path: "/apps/:slug/web-widget/default-customize",
        },
    },
} satisfies Meta<typeof AppWebWidgetDefaultCustomize>;

export default meta;

type Story = StoryObj<typeof meta>;

// Nothing saved yet (the common first visit): the built-in default.
export const Default: Story = {};

// An app-wide default the operator has already tailored.
export const WithSavedTheme: Story = {
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getDefaultTheme({
                        theme: SAMPLE_CHANNEL_THEME,
                        fontOptions: SAMPLE_FONT_OPTIONS,
                    }),
                    ...iframeHandlers(),
                ],
            },
        },
    },
};

// A dark app-wide default.
export const DarkTheme: Story = {
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getDefaultTheme({
                        theme: {
                            ...SAMPLE_CHANNEL_THEME,
                            appearance: "Dark",
                            dark: { buttonColor: "#0f766e", messageColor: "#122d2a", backgroundColor: "#0d1117" },
                        },
                        fontOptions: SAMPLE_FONT_OPTIONS,
                    }),
                    ...iframeHandlers(),
                ],
            },
        },
    },
};
