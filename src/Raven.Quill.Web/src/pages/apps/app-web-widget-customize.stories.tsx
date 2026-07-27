import type { Meta, StoryObj } from "@storybook/react-vite";
import { SAMPLE_CHANNEL_ID } from "@/mocks/channels-mocks";
import { iframeHandlers, iframeMocks, SAMPLE_DEFAULT_CSS } from "@/mocks/iframe-mocks";
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

// The widget has its own saved custom CSS: the "Custom CSS" card is selected and the editor shows it.
export const Default: Story = {};

// The widget follows the app default (no choice of its own): the "App default" card is selected
// and describes what the app default currently resolves to.
export const FollowsAppDefault: Story = {
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getCustomization({
                        style: null,
                        css: null,
                        defaultStyle: "Custom",
                        defaultCss: SAMPLE_DEFAULT_CSS,
                    }),
                    ...iframeHandlers(),
                ],
            },
        },
    },
};

// The widget uses a built-in preset: the preview renders the Dark theme with no CSS editor.
export const DarkPreset: Story = {
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getCustomization({
                        style: "Dark",
                        css: null,
                        defaultStyle: "Light",
                        defaultCss: null,
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
