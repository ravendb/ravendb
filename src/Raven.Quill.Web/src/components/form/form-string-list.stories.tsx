// The harness reads `watch()` on every render to echo the committed order for assertions; React
// Compiler memoization would freeze that read against react-hook-form's mutable internal state.
"use no memo";

import { useForm } from "react-hook-form";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { FormStringList } from "./form-string-list";

type Values = { prompts: { value: string }[] };

// dnd-kit only registers a keyboard lift, and each subsequent arrow move, asynchronously. Firing the next
// key right after can beat that update and silently land as a no-op. `data-dragging` flips as soon as the
// lift actually lands, and stays true for the whole drag, so it is a reliable signal for the lift. There is
// no equivalent DOM attribute per move, but dnd-kit's screen reader live region re-announces on every
// collision recalculation ("... was moved over droppable area ..."), including the initial self-over
// announcement the lift itself produces, so waiting for that text to change (rather than merely appear)
// confirms an arrow key's move actually landed.

/** Resolves once the lift has landed, returning the announcement text at that point so a later
 *  `waitForMove` call can tell a real move apart from that same, already-stale text. */
async function waitForLift(canvasElement: HTMLElement): Promise<string> {
    await waitFor(() => expect(canvasElement.querySelector("[data-dragging]")).not.toBeNull());
    return canvasElement.querySelector('[role="status"]')?.textContent ?? "";
}

async function waitForMove(canvasElement: HTMLElement, announcementBeforeMove: string) {
    await waitFor(() => {
        const status = canvasElement.querySelector('[role="status"]')?.textContent ?? "";
        expect(status).toMatch(/was moved over droppable area/);
        expect(status).not.toBe(announcementBeforeMove);
    });
}

/** Mirrors how the theme editor drives it: a field array of `{ value }` rows, echoed so a play function can
 *  read the committed order without reaching into react-hook-form. */
function Harness({
    sortable,
    label = "Prompts",
    values = ["first", "second", "third"],
}: {
    sortable?: boolean;
    label?: string;
    values?: string[];
}) {
    const { control, watch } = useForm<Values>({
        defaultValues: { prompts: values.map((value) => ({ value })) },
    });

    return (
        <div className="max-w-md p-6">
            <FormStringList
                control={control}
                name="prompts"
                label={label}
                addButtonLabel="Add prompt"
                emptyLabel="No prompts."
                defaultValue={{ value: "" }}
                fieldName={(index) => `prompts.${index}.value` as const}
                sortable={sortable}
            />
            <output data-testid="order">
                {watch("prompts")
                    .map((prompt) => prompt.value)
                    .join(",")}
            </output>
        </div>
    );
}

const meta = {
    title: "Components/Form String List",
    component: Harness,
} satisfies Meta<typeof Harness>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
export const Sortable: Story = { args: { sortable: true } };

// Keyboard rather than pointer: a synthetic drag is unreliable in the browser runner, and dnd-kit routes
// both sensors into the same `fieldArray.move` call, so this covers the reorder either way.
export const ReordersWithTheKeyboard: Story = {
    tags: ["!dev"],
    args: { sortable: true },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        expect(canvas.getByTestId("order")).toHaveTextContent("first,second,third");

        canvas.getByRole("button", { name: "Reorder prompts 1" }).focus();
        await userEvent.keyboard("{ }");
        const liftAnnouncement = await waitForLift(canvasElement);

        await userEvent.keyboard("{ArrowDown}");
        await waitForMove(canvasElement, liftAnnouncement);
        await userEvent.keyboard("{ }");

        await waitFor(() => expect(canvas.getByTestId("order")).toHaveTextContent("second,first,third"));
    },
};

// Two reorders back to back: `useFieldArray`'s `move` must keep each row's own field id stable across a
// move, not regenerate it. A regenerated id would desync dnd-kit's `SortableContext` from react-hook-form's
// rows, and a single drag would not show that, only a second one landing on the wrong row would.
export const ReordersTwiceWithTheKeyboard: Story = {
    tags: ["!dev"],
    args: { sortable: true },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        expect(canvas.getByTestId("order")).toHaveTextContent("first,second,third");

        canvas.getByRole("button", { name: "Reorder prompts 1" }).focus();
        await userEvent.keyboard("{ }");
        const liftAnnouncement = await waitForLift(canvasElement);

        await userEvent.keyboard("{ArrowDown}");
        await waitForMove(canvasElement, liftAnnouncement);
        await userEvent.keyboard("{ }");

        await waitFor(() => expect(canvas.getByTestId("order")).toHaveTextContent("second,first,third"));

        canvas.getByRole("button", { name: "Reorder prompts 2" }).focus();
        await userEvent.keyboard("{ }");
        const secondLiftAnnouncement = await waitForLift(canvasElement);

        await userEvent.keyboard("{ArrowDown}");
        await waitForMove(canvasElement, secondLiftAnnouncement);
        await userEvent.keyboard("{ }");

        await waitFor(() => expect(canvas.getByTestId("order")).toHaveTextContent("second,third,first"));
    },
};

// The next task passes a multi-word label and queries for its lowercased handle name, so this pins that
// the accessible name lowercases the whole label rather than, say, only its first word.
export const HandleNameLowercasesAMultiWordLabel: Story = {
    tags: ["!dev"],
    args: { sortable: true, label: "Suggested prompts" },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        expect(canvas.getByRole("button", { name: "Reorder suggested prompts 1" })).toBeInTheDocument();
    },
};

// A single row has nowhere to move to, so the handle would be a control that does nothing. This is the
// one-row / two-row boundary: it must hold even though `sortable` itself is on.
export const HasNoHandleWithOneRow: Story = {
    tags: ["!dev"],
    args: { sortable: true, values: ["only"] },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        expect(canvas.queryByRole("button", { name: /^Reorder/ })).toBeNull();
    },
};

// Without `sortable` the list is the plain one seven other call sites render, and nothing about it changes.
export const HasNoHandlesWhenNotSortable: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        expect(canvas.queryByRole("button", { name: /^Reorder/ })).toBeNull();
        expect(canvas.getAllByRole("button", { name: "Remove value" })).toHaveLength(3);
    },
};
