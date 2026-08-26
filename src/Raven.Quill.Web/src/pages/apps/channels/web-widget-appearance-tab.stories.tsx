import type { Decorator, Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { Link } from "react-router";
import type { WidgetTheme } from "@/api/generated/server-api";
import { SAMPLE_CHANNEL_ID } from "@/mocks/channels-mocks";
import {
    iframeHandlers,
    iframeMocks,
    SAMPLE_CHANNEL_THEME,
    SAMPLE_DEFAULT_THEME,
    SAMPLE_FONT_OPTIONS,
    statefulThemeMocks,
} from "@/mocks/iframe-mocks";
import { MAX_SUGGESTED_PROMPTS } from "@/pages/apps/channels/web-widget-theme-schema";
import { WebWidgetAppearanceTab } from "./web-widget-appearance-tab";

// In production this tab renders inside the channel detail's tab panel, which app-channel-detail.tsx
// gives `min-h-0 flex-1` inside a `h-full min-h-0` Tabs root - a bounded box with no scroller of its
// own. Rendering the tab by itself has to reproduce that, or the editor grows to its content and the
// layout stories below have no bounded pane left to prove anything about.
const withChannelDetailTabPanel: Decorator = (Story) => (
    <div className="flex h-svh min-h-0 flex-col bg-background p-2 text-foreground">
        <div className="flex min-h-0 flex-1 flex-col">
            <Story />
        </div>
    </div>
);

const meta = {
    title: "Apps/Channels/Web widget appearance tab",
    component: WebWidgetAppearanceTab,
    decorators: [withChannelDetailTabPanel],
    parameters: {
        // Named sizes the layout stories below opt into via `defaultViewport`. addon-vitest otherwise
        // pins every story to 1200x900 (DEFAULT_VIEWPORT_DIMENSIONS in its vitest plugin) regardless of
        // what a story actually needs to prove - at that size the stage lands around 775px tall, tall
        // enough that a reintroduced fixed-height (F1) preview frame would still fit with slack.
        viewport: {
            options: {
                // Wide enough to be in two-pane mode, short enough that a reverted F1 (a preview frame
                // fixed at 640px again instead of filling the stage) would visibly overflow it.
                themeEditorTwoPaneShort: {
                    name: "Theme editor: two-pane, short",
                    styles: { width: "1280px", height: "700px" },
                },
                // Under the @5xl/theme-editor container threshold on its own, regardless of this
                // viewport's generous height.
                themeEditorNarrow: {
                    name: "Theme editor: narrow",
                    styles: { width: "800px", height: "900px" },
                },
                // Wide enough to be in two-pane mode by viewport alone - only pairs with a decorator that
                // narrows the form's own container to prove the split reads that, not the viewport.
                themeEditorWide: {
                    name: "Theme editor: wide",
                    styles: { width: "1280px", height: "900px" },
                },
            },
        },
    },
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
/**
 * Colours are rows now, not fields: the row is the picker's trigger and the value lives inside the
 * popover it opens, so a story that reads or writes one has to go through it. The Colors section shows
 * one scheme at a time - the one the preview is on - so the trigger is named for the colour alone.
 *
 * The trigger's accessible name now embeds its current hex value (e.g. "Button #ff775f") so a
 * screen-reader user can read a row's colour without opening it - so the query matches only the
 * stable, colour-independent prefix rather than the whole name.
 */
function colorPickerName(name: string): RegExp {
    return new RegExp(`^${name}\\b`);
}

async function openColorPicker(canvas: ReturnType<typeof within>, name: string) {
    await userEvent.click(await canvas.findByRole("button", { name: colorPickerName(name) }));
    return within(document.body).findByLabelText("HEX value");
}

async function readColor(canvas: ReturnType<typeof within>, name: string) {
    const field = (await openColorPicker(canvas, name)) as HTMLInputElement;
    const value = field.value;
    await userEvent.keyboard("{Escape}");
    return value;
}

async function setColor(canvas: ReturnType<typeof within>, name: string, hex: string) {
    // Closes via the trigger, not Escape: Escape now cancels (restores the colour from when the
    // popover opened) rather than committing, so it would undo the very edit this helper makes.
    const trigger = await canvas.findByRole("button", { name: colorPickerName(name) });
    await userEvent.click(trigger);
    const field = await within(document.body).findByLabelText("HEX value");
    await userEvent.clear(field);
    await userEvent.type(field, hex);
    await userEvent.click(trigger);
}

// Editing then leaving must be intercepted: this form is long, and losing it silently is the
// worst thing the screen can do. The story router is a data router so `useBlocker` works.
export const GuardsUnsavedChanges: Story = {
    tags: ["!dev"],
    // The tab carries no navigation of its own - the channel detail's header does - so the story
    // supplies the link, which is all the guard actually reacts to: a route change while dirty.
    decorators: [
        (Story) => (
            <>
                <Link to="/elsewhere">Leave this page</Link>
                <Story />
            </>
        ),
    ],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        // Colors is the only section open by default, so its swatches are the ones reliably mounted.
        await setColor(canvas, "Button", "#123456");

        await userEvent.click(canvas.getByRole("link", { name: "Leave this page" }));

        await waitFor(() => expect(within(document.body).getByText("Discard unsaved changes?")).toBeInTheDocument());
    },
};

// Save must say whether there is anything to save, and Discard must put the form back without a
// round trip - the operator's only other way out is navigating away and confirming a dialog.
export const DiscardsChanges: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        // Colors is the only section open by default, so its swatches are the ones reliably mounted.
        const originalColor = await readColor(canvas, "Button");

        expect(canvas.getByRole("button", { name: "Save" })).toBeDisabled();
        expect(canvas.queryByRole("button", { name: "Discard changes" })).not.toBeInTheDocument();

        await setColor(canvas, "Button", "#123456");

        await waitFor(() => expect(canvas.getByRole("button", { name: "Save" })).toBeEnabled());

        // A null save while dirty would race the re-seed against markSaved and could baseline unsent
        // edits as saved, so the escape hatch stays closed until the form is clean again.
        expect(canvas.getByRole("button", { name: "Follow app default" })).toBeDisabled();

        await userEvent.click(canvas.getByRole("button", { name: "Discard changes" }));

        // The section's own reset button only renders while that section is dirty, so it unmounting is
        // the signal that Colors is back - and a settled form is what makes reading the swatch safe.
        await waitFor(() => expect(canvas.queryByRole("button", { name: "Reset Colors section" })).toBeNull());
        expect(await readColor(canvas, "Button")).toBe(originalColor);
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
        await setColor(canvas, "Button", "#123456");

        await waitFor(() => expect(canvas.getByRole("button", { name: "Save" })).toBeEnabled());
        await userEvent.click(canvas.getByRole("button", { name: "Save" }));

        // The form must be clean again before the escape hatch is even offered.
        await waitFor(() => expect(canvas.getByRole("button", { name: "Follow app default" })).toBeEnabled());

        await userEvent.click(canvas.getByRole("button", { name: "Follow app default" }));

        // Reading a swatch means opening its popover, so this waits on the save settling and reads once
        // rather than polling - a waitFor around readColor would reopen the popover on every retry.
        await waitFor(() => expect(canvas.getByRole("button", { name: "Save" })).toBeDisabled());
        expect(await readColor(canvas, "Button")).toBe(SAMPLE_DEFAULT_THEME.light.buttonColor);
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
        await setColor(canvas, "Button", "#123456");

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
        const originalColor = await readColor(canvas, "Button");

        await userEvent.click(canvas.getByRole("button", { name: "Style" }));
        // Radius and Font size are both segmented radio groups whose steps share names ("Small",
        // "Medium", "Large"), so the query has to be scoped to the group rather than the section.
        const radius = within(await canvas.findByRole("radiogroup", { name: "Radius" }));
        // The fixture theme already saves "Large", so that has to be the one value NOT picked here -
        // otherwise the assertion below would be vacuously true.
        expect(radius.getByRole("radio", { name: "Large" })).toBeChecked();

        await setColor(canvas, "Button", "#123456");
        await userEvent.click(radius.getByRole("radio", { name: "None" }));

        await waitFor(() => expect(canvas.getByRole("button", { name: "Save" })).toBeEnabled());

        await userEvent.click(canvas.getByRole("button", { name: "Reset Colors section" }));

        // Colors goes back; Style keeps the edit, and the form is still dirty because of it.
        // The section's own reset button only renders while that section is dirty, so it unmounting is
        // the signal that Colors is back - and a settled form is what makes reading the swatch safe.
        await waitFor(() => expect(canvas.queryByRole("button", { name: "Reset Colors section" })).toBeNull());
        expect(await readColor(canvas, "Button")).toBe(originalColor);
        expect(radius.getByRole("radio", { name: "None" })).toBeChecked();
        expect(canvas.getByRole("button", { name: "Save" })).toBeEnabled();
    },
};

// The host page sizes the iframe, so "does my greeting wrap badly in a narrow sidebar?" is a real
// question. 320 is where a prompt pill first wraps.
export const PreviewsNarrowEmbedWidth: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const frame = await canvas.findByTitle("Web widget preview");

        await userEvent.click(canvas.getByRole("radio", { name: "Mobile" }));

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

        const contentTrigger = await canvas.findByRole("button", { name: "Content" });
        await userEvent.click(contentTrigger);
        const promptsBefore = canvas.getAllByPlaceholderText("Where is my order?").length;

        await userEvent.click(canvas.getByRole("button", { name: "Add prompt" }));
        await waitFor(() =>
            expect(canvas.getAllByPlaceholderText("Where is my order?")).toHaveLength(promptsBefore + 1),
        );

        await userEvent.click(canvas.getByRole("button", { name: "Reset Content section" }));

        await waitFor(() => expect(canvas.getAllByPlaceholderText("Where is my order?")).toHaveLength(promptsBefore));
        // The reset button only renders while dirty, so this click unmounts it - onResetClick
        // (theme-editor-section.tsx) moves focus to the section's own trigger deliberately. If that ref
        // were ever dropped, this would silently fall through to <body> instead of failing loudly.
        expect(document.activeElement).toBe(contentTrigger);
    },
};

// The stories below read layout geometry rather than matching class-name strings: a `querySelector`
// keyed on `[class*="@container/stage"]` only proves the class is still spelled that way, not that the
// stage actually fits its content - a harmless rename would break the selector (failing loudly, which
// is something) but a regression that leaves the class in place and breaks the CSS behind it would sail
// through unnoticed. Walking the DOM from stable, semantic anchors (a section's own heading button, a
// visible label) and reading computed style/geometry off what that reaches catches the latter too.
function getThemeEditorPanes(canvasElement: HTMLElement) {
    const canvas = within(canvasElement);

    // Colors is the only section open by default (ColorsSection sets defaultOpen), so its <section> -
    // the Collapsible's own root - is always mounted. From there: its parent is ThemeEditorInspector's
    // divided list of sections, whose parent is the inspector pane itself, whose parent is the row
    // shared with the stage.
    const colorsSection = canvas.getByRole("button", { name: "Colors" }).closest("section")!;
    const inspector = colorsSection.parentElement!.parentElement!;
    const layoutRow = inspector.parentElement!;

    // The preview frame is the one thing the stage exists to show, and it carries a stable title. Its
    // parent is the width box, whose parent is the dot-grid canvas, whose parent is the stage pane.
    // (The stage's controls float over that canvas out of flow, so they are no use as an anchor for
    // geometry read off the pane.)
    const stage = canvas.getByTitle("Web widget preview").parentElement!.parentElement!.parentElement!;

    return { layoutRow, inspector, stage };
}

// addon-vitest's default 1200x900 leaves the stage ~775px tall - tall enough that a reverted F1 (the
// preview frame fixed at 640px again instead of filling the stage) would still fit with ~135px to
// spare, so the overflow this checks for would never actually show up. themeEditorTwoPaneShort trims
// the viewport to 700px tall instead, leaving well under 640px for the stage once its own controls row
// and padding are accounted for, so a reintroduced fixed-height frame overflows for real.
//
// Proved this can fail: reverting only the preview's own className to a bare "h-[640px]" turned out not
// to be enough - the surrounding wrapper (still "flex flex-col" with the row above it still stretching)
// flex-shrinks an explicitly-sized flex child to fit regardless, so the frame quietly gave up its 640px
// and the assertion kept passing. Reproducing F1 for real needs the wrapper reverted too: dropped
// "flex flex-col" back to plain "max-w-full" on the width box and "@5xl/theme-editor:items-stretch" from
// the row above it (its pre-fix shape - see git history), which stops the frame from being coerced down.
// With both reverted, this story failed with `AssertionError: expected 712 to be less than or equal to
// 573` (`stage.scrollHeight` vs `stage.clientHeight`). Reverted immediately after.
export const StageFillsAvailableHeightWithoutOverflow: Story = {
    tags: ["!dev"],
    parameters: { viewport: { defaultViewport: "themeEditorTwoPaneShort" } },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await canvas.findByTitle("Web widget preview");

        const { layoutRow, stage } = getThemeEditorPanes(canvasElement);

        // The two-pane split has to actually be active for "the stage doesn't overflow its share of it"
        // to mean anything - otherwise this would trivially pass in stacked mode too.
        await waitFor(() => expect(getComputedStyle(layoutRow).display).toBe("grid"));

        await waitFor(() => expect(stage.scrollHeight).toBeLessThanOrEqual(stage.clientHeight));
    },
};

// Companion to the assertion above: the inspector is the one pane that is *supposed* to scroll on its
// own (it can hold far more sections than fit), so proving the stage doesn't scroll only matters
// alongside proving the inspector still does. Both regressed together pre-fix (F2): the two-pane grid
// stretched both panes to equal, too-short rows, so neither the inspector's own scroll nor the stage's
// overflow behaved as intended.
//
// Proved this can fail: temporarily dropped `@5xl/theme-editor:overflow-y-auto` from the inspector's
// className in theme-editor-inspector.tsx (leaving `@5xl/theme-editor:min-h-0 @5xl/theme-editor:flex-1`
// - a bounded pane that still isn't allowed to scroll) and ran this story alone - the computed
// `overflow-y` came back `"visible"` against the expected `"auto"`, failing with
// `AssertionError: expected 'visible' to be 'auto'`. Reverted immediately after.
export const InspectorIsTheOnlyScrollingRegion: Story = {
    tags: ["!dev"],
    parameters: { viewport: { defaultViewport: "themeEditorTwoPaneShort" } },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await canvas.findByTitle("Web widget preview");

        const { layoutRow, inspector, stage } = getThemeEditorPanes(canvasElement);

        await waitFor(() => expect(getComputedStyle(layoutRow).display).toBe("grid"));

        // The inspector has enough sections to outgrow the pane at this viewport, so it should scroll…
        await waitFor(() => expect(getComputedStyle(inspector).overflowY).toBe("auto"));
        await waitFor(() => expect(inspector.scrollHeight).toBeGreaterThan(inspector.clientHeight));
        // …and the stage, sized to fit rather than clipped or stretched thin, should not have to.
        expect(stage.scrollHeight).toBeLessThanOrEqual(stage.clientHeight);
    },
};

// F2 fixed the two-pane grid forcing a bounded height below its own breakpoint; nothing until now
// proved the stacked branch itself - that below @5xl/theme-editor the row is a plain flex column (not a
// one-column grid still carrying `min-h-0 flex-1`) and neither pane gets its own scroll region, so the
// page scrolls once instead.
//
// Proved this can fail: temporarily restored the pre-F3 shape of the row in theme-editor.tsx - `"grid
// min-h-0 flex-1 lg:grid-cols-[minmax(0,26rem)_minmax(0,1fr)]"` (unconditionally a grid, only the
// column split gated) - and ran this story alone at the same 800px-wide viewport. `getComputedStyle
// (layoutRow).display` came back `"grid"` against the expected `"flex"`, failing with
// `AssertionError: expected 'grid' to be 'flex'`. Reverted immediately after.
export const StacksBelowTheContainerThreshold: Story = {
    tags: ["!dev"],
    parameters: { viewport: { defaultViewport: "themeEditorNarrow" } },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await canvas.findByTitle("Web widget preview");

        const { layoutRow, inspector } = getThemeEditorPanes(canvasElement);

        await waitFor(() => expect(getComputedStyle(layoutRow).display).toBe("flex"));
        // Neither pane gets its own bounded, scrolling region below the threshold...
        expect(getComputedStyle(inspector).overflowY).not.toBe("auto");
        // ...the stacked row is the one scroller instead, because the tab panel hosting it has none.
        await waitFor(() => expect(getComputedStyle(layoutRow).overflowY).toBe("auto"));
        await waitFor(() => expect(layoutRow.scrollHeight).toBeGreaterThan(layoutRow.clientHeight));
    },
};

// F3 fixed the split being gated on viewport width via `lg:` instead of the width the editor's own
// container actually has - the assistant panel pinned open is exactly the state that used to force an
// unusably narrow two-pane split. A wide viewport paired with a decorator that narrows only the story's
// own container (standing in for the panel eating into the routed content's width) is enough to prove
// the split reads the container, not the viewport - mounting the real assistant panel isn't needed.
//
// Proved this can fail: with the same pre-F3 row shape as above (`"grid min-h-0 flex-1
// lg:grid-cols-[...]"`), the unconditional `grid` already fails this the same way as
// StacksBelowTheContainerThreshold above - but the state this story exists to catch is the *column*
// split firing on viewport width despite the narrow container, so `lg:grid-cols-[...]` was the relevant
// part: at this story's 1280px viewport (>= lg's 1024px) the column split fired even though the
// decorator holds the container to 700px. Confirmed via the same `display` check: `"grid"` where
// `"flex"` was expected. Reverted immediately after.
export const StacksInAConstrainedContainerDespiteAWideViewport: Story = {
    tags: ["!dev"],
    parameters: { viewport: { defaultViewport: "themeEditorWide" } },
    decorators: [
        (Story) => (
            <div style={{ width: "700px" }}>
                <Story />
            </div>
        ),
    ],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await canvas.findByTitle("Web widget preview");

        const { layoutRow } = getThemeEditorPanes(canvasElement);

        await waitFor(() => expect(getComputedStyle(layoutRow).display).toBe("flex"));
    },
};

// The picker's presets are an operator's two useful anchors for a colour: what the product ships and
// what this channel currently has saved. SAMPLE_CHANNEL_THEME's light button colour differs from the
// default, so both swatches are present and distinct here.
//
// The default preset here is asserted against a `defaultTheme` fixture whose light button colour
// (`#123abc`) deliberately differs from SAMPLE_DEFAULT_THEME's (`#ff775f`). If the anchor were read
// from the built-in DEFAULT_THEME instead of the app default this mock supplies, this story would
// offer "#ff775f" and this assertion would fail - the two are no longer coincidentally equal.
const CUSTOM_APP_DEFAULT_THEME: WidgetTheme = {
    ...SAMPLE_DEFAULT_THEME,
    light: { ...SAMPLE_DEFAULT_THEME.light, buttonColor: "#123abc" },
};

export const OffersDefaultAndSavedPresets: Story = {
    tags: ["!dev"],
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getTheme({
                        theme: SAMPLE_CHANNEL_THEME,
                        defaultTheme: CUSTOM_APP_DEFAULT_THEME,
                        fontOptions: SAMPLE_FONT_OPTIONS,
                    }),
                    ...iframeHandlers(),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        // The edit must happen before the assertion: right after load, the live form values equal the
        // saved theme, so a saved-preset anchor mistakenly wired to live form state would still pass.
        // Editing first and then reopening the picker is the only way to tell the two apart.
        await setColor(canvas, "Button", "#123456");

        await userEvent.click(await canvas.findByRole("button", { name: colorPickerName("Button") }));

        const surface = within(document.body);
        await surface.findByRole("button", { name: `Use ${CUSTOM_APP_DEFAULT_THEME.light.buttonColor}` });
        await surface.findByRole("button", { name: `Use ${SAMPLE_CHANNEL_THEME.light.buttonColor}` });
        await userEvent.keyboard("{Escape}");

        // Switching schemes must move the anchors with it: Dark's presets come from the same two
        // fixtures' `dark` colours, not a leftover copy of Light's.
        await userEvent.click(
            within(canvas.getByRole("radiogroup", { name: "Scheme being edited" })).getByRole("radio", {
                name: "Dark",
            }),
        );
        await userEvent.click(await canvas.findByRole("button", { name: colorPickerName("Button") }));
        await surface.findByRole("button", { name: `Use ${CUSTOM_APP_DEFAULT_THEME.dark.buttonColor}` });
        await surface.findByRole("button", { name: `Use ${SAMPLE_CHANNEL_THEME.dark.buttonColor}` });
    },
};

// The common case: a channel that has never been customised has its saved colour equal to the app
// default (theme is null, so savedTheme falls back to defaultTheme - the very object identity
// presetsFor's dedup exists to handle). Offering the same swatch twice here would read like a bug.
export const OffersOnlyOnePresetWhenDefaultAndSavedMatch: Story = {
    tags: ["!dev"],
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
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(await canvas.findByRole("button", { name: colorPickerName("Button") }));

        const surface = within(document.body);
        // The curated palettes share this row, so counting every swatch would prove nothing. What the
        // dedup is responsible for is this one colour appearing once rather than twice.
        const anchor = new RegExp(`^Use ${SAMPLE_DEFAULT_THEME.light.buttonColor}$`, "i");
        expect(await surface.findAllByRole("button", { name: anchor })).toHaveLength(1);
    },
};

// presetsFor compares anchors case-insensitively, so a saved "#FF775F" doesn't slip past a default
// "#ff775f" as a second, visually identical swatch. Only the button colour differs from
// SAMPLE_CHANNEL_THEME, and only by case, from SAMPLE_DEFAULT_THEME's.
const CASE_DIFFERS_ONLY_THEME: WidgetTheme = {
    ...SAMPLE_CHANNEL_THEME,
    light: { ...SAMPLE_CHANNEL_THEME.light, buttonColor: SAMPLE_DEFAULT_THEME.light.buttonColor.toUpperCase() },
};

export const DedupesPresetsThatOnlyDifferByCase: Story = {
    tags: ["!dev"],
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getTheme({
                        theme: CASE_DIFFERS_ONLY_THEME,
                        defaultTheme: SAMPLE_DEFAULT_THEME,
                        fontOptions: SAMPLE_FONT_OPTIONS,
                    }),
                    ...iframeHandlers(),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(await canvas.findByRole("button", { name: colorPickerName("Button") }));

        const surface = within(document.body);
        // Matched case-insensitively on purpose: the point is that "#FF775F" and "#ff775f" collapse to
        // one swatch, so a case-sensitive query would pass even if both were rendered.
        const anchor = new RegExp(`^Use ${SAMPLE_DEFAULT_THEME.light.buttonColor}$`, "i");
        expect(await surface.findAllByRole("button", { name: anchor })).toHaveLength(1);
    },
};

/** The rows of the list being dragged that dnd-kit's sorting strategy has translated out of the way.
 *  Every row carries a transform for the whole drag, so only a non-zero vertical offset counts. */
function displacedSiblings(canvasElement: HTMLElement) {
    const dragged = canvasElement.querySelector("[data-dragging]");
    const rows = Array.from(dragged?.parentElement?.children ?? []) as HTMLElement[];
    return rows.filter((row) => {
        if (row === dragged) return false;
        const offset = /translate3d\([^,]+,\s*(-?[\d.]+)px/.exec(row.style.transform);
        return offset !== null && Number(offset[1]) !== 0;
    });
}

// The prompts are read in order on the welcome screen, so the editor has to offer a way to change it that
// is not "retype every row below this one".
export const ReordersSuggestedPrompts: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(await canvas.findByRole("button", { name: "Content" }));

        const handle = () => canvas.getByRole("button", { name: "Reorder suggested prompts 1" });
        const promptInputs = () =>
            canvas
                .getAllByRole("textbox")
                .filter((input): input is HTMLInputElement =>
                    SAMPLE_CHANNEL_THEME.suggestedPrompts.includes((input as HTMLInputElement).value),
                );

        // The section animates open, so its rows are still moving for a moment after the click. dnd-kit
        // measures the droppable rects as the drag lifts, and a mid-animation measurement makes that
        // first collision pass land on the row below - which then reads as the drag having travelled two
        // rows instead of one. Two identical measurements in a row mean the geometry has stopped moving.
        let lastTop = Number.NaN;
        await waitFor(() => {
            const top = handle().getBoundingClientRect().top;
            const isSettled = top === lastTop;
            lastTop = top;
            expect(isSettled).toBe(true);
        });

        handle().focus();
        await userEvent.keyboard("{ }");

        // dnd-kit only registers the lift asynchronously, so a bare "{ArrowDown}" right after can land
        // before the lift and silently do nothing. `data-dragging` is set once dnd-kit's `isDragging`
        // actually flips.
        await waitFor(() => expect(canvasElement.querySelector("[data-dragging]")).not.toBeNull());

        await userEvent.keyboard("{ArrowDown}");

        // Same story for the move itself: dropping right after the arrow key can beat dnd-kit's collision
        // recalculation, committing before `over` has updated to the row underneath. The sorting strategy
        // translates whichever row is being displaced, so a neighbour carrying a transform is the proof
        // that the move has been taken into account and the drop has somewhere to land.
        await waitFor(() => expect(displacedSiblings(canvasElement)).not.toHaveLength(0));

        await userEvent.keyboard("{ }");
        await waitFor(() => expect(canvasElement.querySelector("[data-dragging]")).toBeNull());

        await waitFor(() =>
            expect(promptInputs().map((input) => input.value)).toEqual([
                SAMPLE_CHANNEL_THEME.suggestedPrompts[1],
                SAMPLE_CHANNEL_THEME.suggestedPrompts[0],
                ...SAMPLE_CHANNEL_THEME.suggestedPrompts.slice(2),
            ]),
        );
    },
};

export const SwitchesThePromptLayout: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(await canvas.findByRole("button", { name: "Content" }));

        // SAMPLE_CHANNEL_THEME already saves "Inline", so clicking "Stacked" first is what proves the
        // control is actually wired: without this step, the click below would find "Inline" already
        // checked and the assertion after it would pass even if onChange were a no-op.
        await userEvent.click(await canvas.findByRole("radio", { name: "Stacked" }));
        await waitFor(() => expect(canvas.getByRole("radio", { name: "Stacked" })).toBeChecked());
        expect(canvas.getByRole("radio", { name: "Inline" })).not.toBeChecked();

        await userEvent.click(canvas.getByRole("radio", { name: "Inline" }));
        await waitFor(() => expect(canvas.getByRole("radio", { name: "Inline" })).toBeChecked());
        expect(canvas.getByRole("radio", { name: "Stacked" })).not.toBeChecked();
    },
};

// The schema rejects an eleventh prompt, but the form used to let an operator add rows without limit and
// only report the problem on save. The add button has to close before the list can outgrow the cap.
export const StopsAddingPromptsAtTheCap: Story = {
    tags: ["!dev"],
    parameters: {
        msw: {
            handlers: {
                iframe: [
                    iframeMocks.getTheme({
                        theme: {
                            ...SAMPLE_CHANNEL_THEME,
                            // One short of the cap, so the button is proven live before it goes disabled.
                            suggestedPrompts: Array.from(
                                { length: MAX_SUGGESTED_PROMPTS - 1 },
                                (_, index) => `Prompt ${index + 1}`,
                            ),
                        },
                        defaultTheme: SAMPLE_DEFAULT_THEME,
                        fontOptions: SAMPLE_FONT_OPTIONS,
                    }),
                    ...iframeHandlers(),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(await canvas.findByRole("button", { name: "Content" }));

        await waitFor(() =>
            expect(canvas.getAllByRole("button", { name: "Remove value" })).toHaveLength(MAX_SUGGESTED_PROMPTS - 1),
        );

        const addPrompt = canvas.getByRole("button", { name: "Add prompt" });
        expect(addPrompt).toBeEnabled();

        await userEvent.click(addPrompt);

        await waitFor(() =>
            expect(canvas.getAllByRole("button", { name: "Remove value" })).toHaveLength(MAX_SUGGESTED_PROMPTS),
        );
        await waitFor(() => expect(canvas.getByRole("button", { name: "Add prompt" })).toBeDisabled());

        // Removing a row reopens it: the cap is a live count, not a one-way latch.
        await userEvent.click(canvas.getAllByRole("button", { name: "Remove value" })[0]);
        await waitFor(() => expect(canvas.getByRole("button", { name: "Add prompt" })).toBeEnabled());
    },
};
