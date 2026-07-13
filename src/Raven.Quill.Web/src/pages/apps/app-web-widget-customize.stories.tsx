import type { Meta, StoryObj } from "@storybook/react-vite";
import { SAMPLE_WEB_WIDGET_ID } from "@/mocks/channels-mocks";
import { iframeHandlers, iframeMocks, SAMPLE_DEFAULT_CSS } from "@/mocks/iframe-mocks";
import { AppWebWidgetCustomize } from "./app-web-widget-customize";

const meta = {
    title: "Apps/Web widget appearance",
    component: AppWebWidgetCustomize,
    parameters: {
        page: { title: "Web widget appearance" },
        router: {
            initialPath: `/apps/demo/web-widget/${SAMPLE_WEB_WIDGET_ID}/customize`,
            path: "/apps/:slug/web-widget/:widgetId/customize",
        },
    },
} satisfies Meta<typeof AppWebWidgetCustomize>;

export default meta;

type Story = StoryObj<typeof meta>;

// The widget has its own saved CSS layered over the app default; "Reset to default" restores the default.
export const Default: Story = {};

// The widget inherits the app default (no CSS of its own): the editor pre-fills with the
// formatted app default, which "Reset to default" restores.
export const InheritsAppDefault: Story = {
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getCustomization({ css: null, defaultCss: SAMPLE_DEFAULT_CSS }),
                    ...iframeHandlers(),
                ],
            },
        },
    },
};

// The widgetId in the URL isn't a web widget in this app — the page shows a not-found alert.
export const UnknownWidget: Story = {
    parameters: {
        router: {
            initialPath: "/apps/demo/web-widget/wgt_unknown/customize",
            path: "/apps/:slug/web-widget/:widgetId/customize",
        },
    },
};
