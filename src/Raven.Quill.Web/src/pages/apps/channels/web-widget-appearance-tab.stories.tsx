import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { SAMPLE_CHANNEL_ID } from "@/mocks/channels-mocks";
import {
    iframeHandlers,
    iframeMocks,
    SAMPLE_CHANNEL_THEME,
    SAMPLE_DEFAULT_THEME,
    SAMPLE_FONT_OPTIONS,
    statefulThemeMocks,
} from "@/mocks/iframe-mocks";
import { WebWidgetAppearanceTab } from "./web-widget-appearance-tab";

const meta = {
    title: "Apps/Channels/Web widget appearance tab",
    component: WebWidgetAppearanceTab,
    args: {
        slug: "demo",
        channelId: SAMPLE_CHANNEL_ID,
    },
} satisfies Meta<typeof WebWidgetAppearanceTab>;

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
// Editing then leaving must be intercepted: this form is long, and losing it silently is the
// worst thing the screen can do. The story router is a data router so `useBlocker` works.
export const GuardsUnsavedChanges: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        // Colors is the only section open by default, so its fields are the ones reliably mounted.
        const buttonColor = await canvas.findByLabelText("Button color");

        await userEvent.clear(buttonColor);
        await userEvent.type(buttonColor, "#123456");

        await userEvent.click(canvas.getByRole("link", { name: "Back to channel" }));

        await waitFor(() => expect(within(document.body).getByText("Discard unsaved changes?")).toBeInTheDocument());
    },
};

// Save must say whether there is anything to save, and Discard must put the form back without a
// round trip - the operator's only other way out is navigating away and confirming a dialog.
export const DiscardsChanges: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        // Colors is the only section open by default, so its fields are the ones reliably mounted.
        const buttonColor = await canvas.findByLabelText("Button color");
        const originalColor = (buttonColor as HTMLInputElement).value;

        expect(canvas.getByRole("button", { name: "Save" })).toBeDisabled();
        expect(canvas.queryByRole("button", { name: "Discard changes" })).not.toBeInTheDocument();

        await userEvent.clear(buttonColor);
        await userEvent.type(buttonColor, "#123456");

        await waitFor(() => expect(canvas.getByRole("button", { name: "Save" })).toBeEnabled());

        // A null save while dirty would race the re-seed against markSaved and could baseline unsent
        // edits as saved, so the escape hatch stays closed until the form is clean again.
        expect(canvas.getByRole("button", { name: "Follow app default" })).toBeDisabled();

        await userEvent.click(canvas.getByRole("button", { name: "Discard changes" }));

        await waitFor(() => expect(buttonColor).toHaveValue(originalColor));
        expect(canvas.getByRole("button", { name: "Save" })).toBeDisabled();
        expect(canvas.getByRole("button", { name: "Follow app default" })).toBeEnabled();
    },
};

// markSaved's reset must not leave dirtyFields behind: a stale dirty field would make the next
// re-seed keep the on-screen value instead of adopting what the server actually saved, so a later
// escape hatch (Follow app default) would display - and eventually resave - a value the server never
// received. The other mocks always answer with the same fixed theme, so a stateful pair is needed here
// to prove the re-seed after the null save actually observes the server's response.
export const FollowAppDefaultAdoptsServerTheme: Story = {
    tags: ["!dev"],
    parameters: {
        msw: {
            handlers: {
                iframe: (() => {
                    const theme = statefulThemeMocks();
                    return [
                        theme.getTheme(),
                        theme.updateTheme(),
                        iframeMocks.getDefaultTheme(),
                        iframeMocks.updateDefaultTheme(),
                    ];
                })(),
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const buttonColor = await canvas.findByLabelText("Button color");

        await userEvent.clear(buttonColor);
        await userEvent.type(buttonColor, "#123456");

        await waitFor(() => expect(canvas.getByRole("button", { name: "Save" })).toBeEnabled());
        await userEvent.click(canvas.getByRole("button", { name: "Save" }));

        // The form must be clean again before the escape hatch is even offered.
        await waitFor(() => expect(canvas.getByRole("button", { name: "Follow app default" })).toBeEnabled());

        await userEvent.click(canvas.getByRole("button", { name: "Follow app default" }));

        await waitFor(() => expect(buttonColor).toHaveValue(SAMPLE_DEFAULT_THEME.light.buttonColor));
    },
};

// The one branch of Task 1's saveTheme that no story reaches: a rejected save must leave the form
// dirty, so the navigation guard stays armed until the work has actually reached the server.
export const FailedSaveKeepsChangesDirty: Story = {
    tags: ["!dev"],
    parameters: {
        msw: {
            handlers: {
                iframe: [iframeMocks.updateThemeError(), ...iframeHandlers()],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const buttonColor = await canvas.findByLabelText("Button color");

        await userEvent.clear(buttonColor);
        await userEvent.type(buttonColor, "#123456");

        await waitFor(() => expect(canvas.getByRole("button", { name: "Save" })).toBeEnabled());
        await userEvent.click(canvas.getByRole("button", { name: "Save" }));

        // Both conditions below already hold before the click's flush even reaches the pending render, so
        // asserting them alone would pass whether or not the save actually failed. Wait for the failure
        // itself to land first - http-client.ts maps the mock's error onto the destructive alert's text -
        // then the form's state actually reflects the rejected save.
        await canvas.findByText("Could not save the theme.");

        // The save failed, so the edit is still unsaved: Save stays live and Discard is still offered.
        expect(canvas.getByRole("button", { name: "Discard changes" })).toBeInTheDocument();
        expect(canvas.getByRole("button", { name: "Save" })).toBeEnabled();
    },
};

// Discard's reset uses keepDirtyValues: false, which wipes react-hook-form's `_fields` and
// `_names.mount` - Discard has only been proven against a plain text input so far, and a
// useFieldArray is the field most likely to misbehave through that reset. Prove it puts the array
// back to its saved rows, not just its scalar fields.
export const DiscardsAddedSuggestedPrompt: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        // Content is collapsed by default; only Colors starts open.
        await userEvent.click(await canvas.findByRole("button", { name: "Content" }));

        const savedRowCount = SAMPLE_CHANNEL_THEME.suggestedPrompts.length;
        await waitFor(() =>
            expect(canvas.getAllByRole("button", { name: "Remove value" })).toHaveLength(savedRowCount),
        );

        await userEvent.click(canvas.getByRole("button", { name: "Add prompt" }));

        await waitFor(() =>
            expect(canvas.getAllByRole("button", { name: "Remove value" })).toHaveLength(savedRowCount + 1),
        );
        await waitFor(() => expect(canvas.getByRole("button", { name: "Save" })).toBeEnabled());

        await userEvent.click(canvas.getByRole("button", { name: "Discard changes" }));

        await waitFor(() =>
            expect(canvas.getAllByRole("button", { name: "Remove value" })).toHaveLength(savedRowCount),
        );
        expect(canvas.getByRole("button", { name: "Save" })).toBeDisabled();
    },
};

// The page used to promise a derived palette the form makes unreachable. Until Slice B restores
// derivation, the description has to match what the screen actually does.
export const DescribesWhatTheScreenDoes: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        expect(await canvas.findByText(/nothing reaches visitors until you save/i)).toBeInTheDocument();
        expect(canvas.queryByText(/the rest of the palette is derived from it/i)).not.toBeInTheDocument();
    },
};

// Each section can be undone on its own. The whole-form Discard is too blunt when an operator has
// deliberately changed four things and regrets one of them.
export const ResetsOneSectionOnly: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const buttonColor = await canvas.findByLabelText("Button color");
        const originalColor = (buttonColor as HTMLInputElement).value;

        await userEvent.click(canvas.getByRole("button", { name: "Style" }));
        // Radius is a shadcn/Radix combobox, not a native <select>: userEvent.selectOptions can't target
        // it, and its options only mount in a body-level portal once opened.
        const radius = await canvas.findByLabelText("Radius");
        const originalRadius = radius.textContent;
        // The fixture theme already saves "Large", so that has to be the one value NOT picked here -
        // otherwise the "differs from original" assertion below would be vacuously true.
        expect(originalRadius).not.toBe("Small");

        await userEvent.clear(buttonColor);
        await userEvent.type(buttonColor, "#123456");
        await userEvent.click(radius);
        await userEvent.click(await within(document.body).findByRole("option", { name: "Small" }));

        await waitFor(() => expect(canvas.getByRole("button", { name: "Save" })).toBeEnabled());

        await userEvent.click(canvas.getByRole("button", { name: "Reset Colors section" }));

        // Colors goes back; Style keeps the edit, and the form is still dirty because of it.
        await waitFor(() => expect(buttonColor).toHaveValue(originalColor));
        expect(radius).toHaveTextContent("Small");
        expect(radius).not.toHaveTextContent(originalRadius ?? "");
        expect(canvas.getByRole("button", { name: "Save" })).toBeEnabled();
    },
};

// Bare layout strips the shell's title and back link, so the host page carries both itself —
// otherwise the operator reaches this screen and cannot leave it without the browser's Back.
export const CarriesItsOwnNavigation: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        // The back link no longer waits on any query, so it can mount before channelsQuery resolves -
        // unlike the link, the heading needs its own await rather than riding the link's.
        expect(await canvas.findByRole("link", { name: "Back to channel" })).toBeInTheDocument();
        expect(await canvas.findByRole("heading", { name: "Website widget" })).toBeInTheDocument();
    },
};

// The back link and title used to live inside the editor, which only mounts once ApiState reaches
// its success branch - a slow theme fetch left the operator stranded with no way back. They now
// live on the host page, above ApiState, so a pending theme query must not take them down with it.
export const KeepsBackLinkWhileThemeLoads: Story = {
    tags: ["!dev"],
    parameters: {
        msw: {
            handlers: {
                iframe: [iframeMocks.getThemePending(), ...iframeHandlers()],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        // Confirms the page is actually still in ApiState's loading branch, not the success one.
        expect(await canvas.findByText("Loading theme...")).toBeInTheDocument();
        expect(canvas.getByRole("link", { name: "Back to channel" })).toBeInTheDocument();
    },
};

// Same regression as above, on the error branch: a failed theme fetch must not strand the operator
// either.
export const KeepsBackLinkWhenThemeErrors: Story = {
    tags: ["!dev"],
    parameters: {
        msw: {
            handlers: {
                iframe: [iframeMocks.getThemeError(), ...iframeHandlers()],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        // Confirms the page is actually in ApiState's error branch, not the success one.
        expect(await canvas.findByText("Could not load the theme")).toBeInTheDocument();
        expect(canvas.getByRole("link", { name: "Back to channel" })).toBeInTheDocument();
    },
};

// The host page sizes the iframe, so "does my greeting wrap badly in a narrow sidebar?" is a real
// question. 320 is where a prompt pill first wraps.
export const PreviewsNarrowEmbedWidth: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const frame = await canvas.findByTitle("Web widget preview");

        await userEvent.click(canvas.getByRole("radio", { name: "320" }));

        await waitFor(() => expect(frame.parentElement).toHaveStyle({ width: "320px" }));
    },
};

// Fields that cannot affect anything should not be on screen. Their values are still kept, so turning
// the header back on restores what was there rather than starting from blank.
export const HidesHeaderFieldsWhenHeaderIsOff: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(await canvas.findByRole("button", { name: "Branding" }));
        const title = await canvas.findByLabelText("Title");
        const originalTitle = (title as HTMLInputElement).value;

        await userEvent.click(canvas.getByRole("switch", { name: "Show the header" }));

        await waitFor(() => expect(canvas.queryByLabelText("Title")).not.toBeInTheDocument());
        expect(canvas.queryByLabelText("Logo radius")).not.toBeInTheDocument();

        await userEvent.click(canvas.getByRole("switch", { name: "Show the header" }));

        // The value survived being hidden — this is a disclosure, not a clear.
        await waitFor(() => expect(canvas.getByLabelText("Title")).toHaveValue(originalTitle));
    },
};

// suggestedPrompts is a useFieldArray, the reset path most likely to leave an orphaned row behind.
// Task 1 proved this by hand; this keeps it proven.
export const ResetsASectionContainingAFieldArray: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(await canvas.findByRole("button", { name: "Content" }));
        const promptsBefore = canvas.getAllByPlaceholderText("Where is my order?").length;

        await userEvent.click(canvas.getByRole("button", { name: "Add prompt" }));
        await waitFor(() =>
            expect(canvas.getAllByPlaceholderText("Where is my order?")).toHaveLength(promptsBefore + 1),
        );

        await userEvent.click(canvas.getByRole("button", { name: "Reset Content section" }));

        await waitFor(() => expect(canvas.getAllByPlaceholderText("Where is my order?")).toHaveLength(promptsBefore));
    },
};
