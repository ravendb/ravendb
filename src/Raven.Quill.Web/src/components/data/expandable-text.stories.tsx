import type { Meta, StoryObj } from "@storybook/react-vite";
import { ExpandableText } from "./expandable-text";

const LONG_TEXT =
    "You are a customer support assistant for an e-commerce platform. " +
    "You can answer questions about products, orders, shipping, and returns. " +
    "Always be concise, friendly, and accurate. When you are unsure, ask a clarifying " +
    "question instead of guessing. Never invent order details or prices; rely only on " +
    "the data available through your tools.";

const meta = {
    title: "Components/Expandable Text",
    component: ExpandableText,
    decorators: [
        (Story) => (
            <div className="max-w-md p-6">
                <Story />
            </div>
        ),
    ],
    args: {
        maxLines: 3,
        className: "text-sm text-muted-foreground",
    },
} satisfies Meta<typeof ExpandableText>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {
    args: { children: LONG_TEXT },
};

export const Short: Story = {
    args: { children: "A short prompt that fits comfortably within the line limit." },
};
