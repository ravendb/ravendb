import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { iframeHandlers, iframeMocks, SAMPLE_CHANNEL_THEME, SAMPLE_FONT_OPTIONS } from "@/mocks/iframe-mocks";
import { AppWebWidgetDefaultCustomize } from "./app-web-widget-default-customize";

const meta = {
    title: "Apps/Default web widget appearance",
    component: AppWebWidgetDefaultCustomize,
    parameters: {
        // Production renders this route bare (isBareLayout/isPageTitleHidden): no host title, no padding,
        // single row. Setting page.title here made the harness render a title + padded two-row layout no
        // real operator ever sees, hiding the overflow F1/F2/F3 actually cause.
        page: { bare: true },
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

// The app default is the one theme with nowhere to fall back to, so it needs its own way home.
// A null save is what the server already treats as "reset to the built-in".
export const ResetsToBuiltInDefault: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(await canvas.findByRole("button", { name: "Reset to built-in default" }));

        await waitFor(() => expect(within(document.body).getByText("Default theme saved")).toBeInTheDocument());
    },
};

// The same fix landed on both host pages; only the channel page was covered. api-state.tsx discards
// children on error, so a back link rendered inside the editor would vanish exactly here.
export const KeepsBackLinkWhenDefaultThemeErrors: Story = {
    tags: ["!dev"],
    parameters: {
        msw: {
            handlers: {
                iframe: [iframeMocks.getDefaultThemeError(), ...iframeHandlers()],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await canvas.findByText("Could not load the default theme");
        expect(canvas.getByRole("link", { name: "Back to channels" })).toBeInTheDocument();
    },
};
