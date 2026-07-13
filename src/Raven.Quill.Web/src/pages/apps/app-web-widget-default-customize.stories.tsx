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

// No default saved yet (the common first visit): the editor pre-fills with the widget's
// formatted base styles, which "Reset to default" restores.
export const Default: Story = {};

// A default has been saved: the editor shows it and "Reset to default" restores the base styles.
export const WithSavedCss: Story = {
    parameters: {
        msw: {
            handlers: {
                iframe: [iframeMocks.getDefaultCustomization({ css: SAMPLE_DEFAULT_CSS }), ...iframeHandlers()],
            },
        },
    },
};
