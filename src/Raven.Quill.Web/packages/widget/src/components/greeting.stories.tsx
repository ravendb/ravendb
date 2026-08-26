import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, within } from "storybook/test";
import type { WidgetSuggestedPromptsLayout } from "@/widget-theme";
import { Greeting } from "./greeting";

const meta = {
    title: "Widget/Greeting",
    component: Greeting,
    args: {
        title: "Need a hand with an order?",
        body: "Ask about delivery, returns or anything else.",
        suggestedPrompts: ["Where is my order?", "How do I return an item?"],
        layout: "Stacked",
        isDisabled: false,
        onSelectPrompt: () => {},
    },
} satisfies Meta<typeof Greeting>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Stacked: Story = {};

export const Inline: Story = { args: { layout: "Inline" } };

// A theme crosses a JSON network boundary with no runtime validation, so a layout outside the union is a
// real possibility, not just a type-system exercise. Casting through `as never` reproduces that: it forces
// both lookup objects to miss and confirms the list still falls back to the stacked classes instead of
// rendering with no layout classes at all.
export const UnknownLayoutFallsBackToStacked: Story = {
    args: { layout: "Bogus" as unknown as WidgetSuggestedPromptsLayout },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        const list = canvas.getByRole("list");
        expect(list.className).toBe("flex flex-col items-start gap-2");

        const [item] = canvas.getAllByRole("listitem");
        expect(item.className).toBe("w-full");
    },
};
