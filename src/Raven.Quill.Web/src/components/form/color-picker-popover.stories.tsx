import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { ColorPickerPopover } from "./color-picker-popover";

/** Mirrors how ThemeEditorColorRow drives it: a controlled hex value the popover writes back to. */
function Harness({ initial = "#2f6f4f" }: { initial?: string }) {
    const [value, setValue] = useState(initial);

    return (
        <div className="p-6">
            <ColorPickerPopover
                value={value}
                onChange={setValue}
                label="Button color"
                presets={["#ff775f", "#2f6f4f"]}
            />
            <output data-testid="committed">{value}</output>
        </div>
    );
}

const meta = {
    title: "Components/Color Picker Popover",
    component: Harness,
} satisfies Meta<typeof Harness>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

// RGB is three fields now, not one "RGB value" field, so each channel is entered and read on its own
// input - named "Red"/"Green"/"Blue" for assistive tech even though the row shows no visible caption.
export const EntersAnRgbValue: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(canvas.getByRole("button", { name: "Button color picker" }));

        const surface = within(document.body);
        await userEvent.click(await surface.findByRole("radio", { name: "RGB" }));

        const red = surface.getByLabelText("Red value");
        const green = surface.getByLabelText("Green value");
        const blue = surface.getByLabelText("Blue value");
        expect(red).toHaveValue(47);
        expect(green).toHaveValue(111);
        expect(blue).toHaveValue(79);

        await userEvent.clear(red);
        await userEvent.type(red, "255");
        await userEvent.clear(green);
        await userEvent.type(green, "119");
        await userEvent.clear(blue);
        await userEvent.type(blue, "95");

        await waitFor(() => expect(canvas.getByTestId("committed")).toHaveTextContent("#ff775f"));
    },
};

// A field that re-derives its text from the committed hex on every keystroke fights the operator:
// integer HSL does not address every hex colour, so "41" can convert back to "40" mid-word and the
// caret jumps. While a field has focus, its draft text is the operator's, not the value's - and that
// now has to hold for each of the three HSL fields independently, not just one shared field.
//
// The harness starts from a colour other than #2f6f4f so the final commit assertion is load bearing:
// hsl(150, 41%, 31%) round-trips to #2f6f4f, a different hex than the initial value, so a pass here
// means the typed HSL was actually parsed and committed across all three fields, not that nothing
// happened.
export const KeepsTheDraftWhileTyping: Story = {
    tags: ["!dev"],
    args: { initial: "#000000" },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(canvas.getByRole("button", { name: "Button color picker" }));

        const surface = within(document.body);
        await userEvent.click(await surface.findByRole("radio", { name: "HSL" }));

        const hue = surface.getByLabelText("Hue value");
        const saturation = surface.getByLabelText("Saturation value");
        const lightness = surface.getByLabelText("Lightness value");

        await userEvent.clear(hue);
        await userEvent.type(hue, "150");
        expect(hue).toHaveValue(150);

        await userEvent.clear(saturation);
        await userEvent.type(saturation, "41");
        expect(saturation).toHaveValue(41);

        await userEvent.clear(lightness);
        await userEvent.type(lightness, "31");
        expect(lightness).toHaveValue(31);

        expect(canvas.getByTestId("committed")).toHaveTextContent("#2f6f4f");
    },
};

// A half typed value must never be committed as a colour, and must never wipe the current one.
export const IgnoresUnparseableInput: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(canvas.getByRole("button", { name: "Button color picker" }));

        const surface = within(document.body);
        const field = await surface.findByLabelText(/value/i);

        await userEvent.clear(field);
        await userEvent.type(field, "#2f");

        expect(canvas.getByTestId("committed")).toHaveTextContent("#2f6f4f");
        expect(field).toHaveValue("#2f");
    },
};

// Escape closes the popover without a blur event ever reaching the field (the element is unmounted
// while focused, not blurred first), so the draft has to be cleared some other way or it survives to
// the next open.
export const DiscardsTheDraftOnEscape: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(canvas.getByRole("button", { name: "Button color picker" }));

        const surface = within(document.body);
        const field = await surface.findByLabelText(/value/i);
        await userEvent.clear(field);
        await userEvent.type(field, "#2f");

        await userEvent.keyboard("{Escape}");
        await waitFor(() => expect(surface.queryByLabelText(/value/i)).not.toBeInTheDocument());

        await userEvent.click(canvas.getByRole("button", { name: "Button color picker" }));
        const reopenedField = await surface.findByLabelText(/value/i);
        expect(reopenedField).toHaveValue("#2f6f4f");
    },
};

export const PicksAPreset: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(canvas.getByRole("button", { name: "Button color picker" }));

        const surface = within(document.body);
        await userEvent.click(await surface.findByRole("button", { name: "Use #ff775f" }));

        await waitFor(() => expect(canvas.getByTestId("committed")).toHaveTextContent("#ff775f"));
    },
};

// Nothing else in this file pins HexColorPicker's onChange wiring: every other story reaches the
// committed value through the text field or a preset button, never through the saturation area
// itself. If that wiring were ever routed through a handler that swallowed or debounced it, every
// other story here would still pass.
//
// A fully simulated drag (userEvent.pointer with multiple intermediate steps) is flaky under
// browser-mode vitest, so the drag is driven directly with two events instead: react-colorful v5's
// Interactive area (rendered here with role="slider", aria-label="Color") listens for its own
// mousedown/mousemove pair rather than PointerEvent, and reads client coordinates straight off
// whatever event it receives, so a synthetic mousedown followed by a mousemove on the window reaches
// the same code a real drag would.
//
// The exact resulting hex is not asserted: it depends on geometry react-colorful computes from the
// saturation area's rendered size, which is incidental to what this story is proving. Asserting only
// that the value changed is what "the drag reaches onChange" means here.
export const DraggingTheSaturationAreaCommitsAColor: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(canvas.getByRole("button", { name: "Button color picker" }));

        const surface = within(document.body);
        const saturation = await surface.findByRole("slider", { name: "Color" });
        const before = canvas.getByTestId("committed").textContent;

        const rect = saturation.getBoundingClientRect();
        const start = { clientX: rect.left + rect.width * 0.5, clientY: rect.top + rect.height * 0.5 };
        const end = { clientX: rect.left + rect.width * 0.9, clientY: rect.top + rect.height * 0.1 };

        saturation.dispatchEvent(
            new MouseEvent("mousedown", { bubbles: true, clientX: start.clientX, clientY: start.clientY }),
        );
        window.dispatchEvent(
            new MouseEvent("mousemove", { bubbles: true, clientX: end.clientX, clientY: end.clientY }),
        );
        window.dispatchEvent(new MouseEvent("mouseup", { bubbles: true, clientX: end.clientX, clientY: end.clientY }));

        await waitFor(() => expect(canvas.getByTestId("committed").textContent).not.toBe(before));
    },
};

// Escape means cancel: it restores whatever colour was committed when the popover was opened, not
// whatever the operator last landed on while looking around. Outside-click and the trigger both stay
// a commit - only Escape carries "never mind" here.
//
// Distinct from DiscardsTheDraftOnEscape above: that story is about the *text draft* in the field
// never surviving Escape. This one is about the *committed colour* itself, which used to survive
// Escape unchanged - an operator who drags around to preview a colour and backs out with Escape kept
// whatever was last under the cursor, with the form now dirty.
export const RestoresTheColorOnEscape: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(canvas.getByRole("button", { name: "Button color picker" }));

        const surface = within(document.body);
        await userEvent.click(await surface.findByRole("button", { name: "Use #ff775f" }));
        await waitFor(() => expect(canvas.getByTestId("committed")).toHaveTextContent("#ff775f"));

        await userEvent.keyboard("{Escape}");

        await waitFor(() => expect(canvas.getByTestId("committed")).toHaveTextContent("#2f6f4f"));
    },
};

// NOTE for reviewers: there is deliberately no story asserting that the HSL tab renders a different
// area from HEX and RGB, because it does not. react-colorful 5.8.0 has a single area component, and
// HslColorPicker converts to HSV and draws the identical saturation/value square: same DOM, same
// aria-label, same aria-valuetext. An earlier revision mounted it anyway; that was removed once the
// premise was checked against the installed source, rather than kept as a swap no test could observe.

// Firefox and Safari have no window.EyeDropper. Rendering nothing there beats a dead control, and this
// story pins that by removing the constructor before opening the popover rather than hoping the test
// browser happens to lack it (recent Chromium ships EyeDropper, so an unconditional absence check would
// be testing the browser, not the component).
export const EyedropperIsAbsentWithoutBrowserSupport: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const original = (window as { EyeDropper?: unknown }).EyeDropper;
        delete (window as { EyeDropper?: unknown }).EyeDropper;

        try {
            const canvas = within(canvasElement);
            await userEvent.click(canvas.getByRole("button", { name: "Button color picker" }));

            const surface = within(document.body);
            await surface.findByLabelText("HEX value");
            expect(surface.queryByRole("button", { name: "Pick from screen" })).not.toBeInTheDocument();
        } finally {
            if (original) (window as { EyeDropper?: unknown }).EyeDropper = original;
        }
    },
};
