import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { Button } from "@/components/shadcn/ui/button";
import { ConversationTranscriptSheet } from "./conversation-transcript-sheet";

const meta = {
    title: "Apps/ConversationTranscriptSheet",
    component: ConversationTranscriptSheet,
    tags: ["!dev"],
    args: {
        slug: "demo",
        // the one mock conversation with no stored last exchange, so its detail carries the long sample transcript
        conversationId: "cv_74oJu",
        agentName: "order-support",
        channelName: "Support Widget",
        trigger: <Button>View transcript</Button>,
    },
} satisfies Meta<typeof ConversationTranscriptSheet>;

export default meta;

type Story = StoryObj<typeof meta>;

// The sheet portals out of the story root, so query the whole document body.
async function openSheet(canvasElement: HTMLElement) {
    await userEvent.click(within(canvasElement).getByRole("button", { name: /view transcript/i }));
    const sheet = await within(document.body).findByRole("dialog");
    await waitFor(() => expect(sheet.querySelector("[data-index]")).toBeInTheDocument());
    return sheet;
}

const rowAt = (sheet: HTMLElement, index: number) => sheet.querySelector<HTMLElement>(`[data-index="${index}"]`);

const requireRowAt = (sheet: HTMLElement, index: number) => {
    const row = rowAt(sheet, index);
    if (row === null) {
        throw new Error(`transcript row ${index} is not rendered`);
    }
    return row;
};

const scrollElementOf = (sheet: HTMLElement) => requireRowAt(sheet, 0).parentElement!.parentElement!;

const SYSTEM_PROMPT_ROW = 1;
const TOOL_CALL_ROW = 3;

export const SystemPromptIsAnOrdinaryTranscriptRow: Story = {
    play: async ({ canvasElement }) => {
        const sheet = await openSheet(canvasElement);

        const systemPrompt = within(requireRowAt(sheet, SYSTEM_PROMPT_ROW)).getByRole("button", {
            name: /system prompt/i,
        });
        expect(within(sheet).queryByText(/you are a helpful store assistant/i)).not.toBeInTheDocument();

        await userEvent.click(systemPrompt);
        expect(await within(sheet).findByText(/you are a helpful store assistant/i)).toBeVisible();
    },
};

// Rows differ wildly in height, so the virtualizer must re-measure a row that grows and push the
// rows below it down instead of leaving them overlapping at their estimated offsets.
export const ExpandingARowRepositionsTheRowsBelowIt: Story = {
    play: async ({ canvasElement }) => {
        const sheet = await openSheet(canvasElement);
        const topOfNextRow = () => requireRowAt(sheet, TOOL_CALL_ROW + 1).style.top;
        const before = topOfNextRow();

        await userEvent.click(
            within(requireRowAt(sheet, TOOL_CALL_ROW)).getByRole("button", { name: /search-products/i }),
        );

        await waitFor(() => expect(parseFloat(topOfNextRow())).toBeGreaterThan(parseFloat(before)));
    },
};

// A row unmounts once it scrolls out of view, which is why the disclosures keep their open state
// outside the row.
export const ExpandedRowsStayExpandedAcrossScrolling: Story = {
    play: async ({ canvasElement }) => {
        const sheet = await openSheet(canvasElement);
        const toolCall = within(requireRowAt(sheet, TOOL_CALL_ROW)).getByRole("button", {
            name: /search-products/i,
        });

        await userEvent.click(toolCall);
        await waitFor(() => expect(toolCall).toHaveAttribute("aria-expanded", "true"));

        const scrollElement = scrollElementOf(sheet);
        scrollElement.scrollTop = scrollElement.scrollHeight;
        await waitFor(() => expect(rowAt(sheet, TOOL_CALL_ROW)).not.toBeInTheDocument());

        scrollElement.scrollTop = 0;
        await waitFor(() => expect(rowAt(sheet, TOOL_CALL_ROW)).toBeInTheDocument());
        expect(
            within(requireRowAt(sheet, TOOL_CALL_ROW)).getByRole("button", { name: /search-products/i }),
        ).toHaveAttribute("aria-expanded", "true");
    },
};
