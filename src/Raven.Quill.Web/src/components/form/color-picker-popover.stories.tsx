import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { ColorPickerPopover } from "./color-picker-popover";

/** Mirrors how FormColorPicker drives it: a controlled hex value the popover writes back to. */
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

export const EntersAnRgbValue: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(canvas.getByRole("button", { name: "Button color picker" }));

        const surface = within(document.body);
        await userEvent.click(await surface.findByRole("radio", { name: "RGB" }));

        const field = surface.getByLabelText("RGB value");
        expect(field).toHaveValue("47, 111, 79");

        await userEvent.clear(field);
        await userEvent.type(field, "255, 119, 95");

        await waitFor(() => expect(canvas.getByTestId("committed")).toHaveTextContent("#ff775f"));
    },
};

// A field that re-derives its text from the committed hex on every keystroke fights the operator:
// integer HSL does not address every hex colour, so "41" can convert back to "40" mid-word and the
// caret jumps. While the field has focus, the draft text is the operator's, not the value's.
//
// The harness starts from a colour other than #2f6f4f so the committed assertion below is load
// bearing: hsl(150, 41%, 31%) round-trips to #2f6f4f, a different hex than the initial value, so a
// pass here means the typed HSL was actually parsed and committed, not that nothing happened.
export const KeepsTheDraftWhileTyping: Story = {
    tags: ["!dev"],
    args: { initial: "#000000" },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.click(canvas.getByRole("button", { name: "Button color picker" }));

        const surface = within(document.body);
        await userEvent.click(await surface.findByRole("radio", { name: "HSL" }));

        const field = surface.getByLabelText("HSL value");
        await userEvent.clear(field);
        await userEvent.type(field, "150, 41%, 31%");

        expect(field).toHaveValue("150, 41%, 31%");
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
