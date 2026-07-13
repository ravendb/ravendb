import type { Meta, StoryObj } from "@storybook/react-vite";
import { iframeHandlers, iframeMocks, SAMPLE_DEFAULT_CSS } from "@/mocks/iframe-mocks";
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

// Nothing saved yet (the common first visit): the Light preset is selected.
export const Default: Story = {};

// A custom-CSS default has been saved: the "Custom CSS" card is selected and the editor shows it.
export const WithSavedCss: Story = {
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getDefaultCustomization({ style: "Custom", css: SAMPLE_DEFAULT_CSS }),
                    ...iframeHandlers(),
                ],
            },
        },
    },
};

// The Dark preset has been saved as the app-wide default.
export const DarkPreset: Story = {
    parameters: {
        msw: {
            handlers: {
                iframe: [iframeMocks.getDefaultCustomization({ style: "Dark", css: null }), ...iframeHandlers()],
            },
        },
    },
};
