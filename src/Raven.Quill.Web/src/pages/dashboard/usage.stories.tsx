import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { sampleTopologyIds, settingsMocks } from "@/mocks/settings-mocks";
import { DashboardUsage } from "./usage";

const meta = {
    title: "Dashboard/Usage",
    component: DashboardUsage,
    parameters: {
        // DashboardUsage renders its own "Usage" header with the month picker beside
        // it, so the shell decorator wraps it without a title of its own.
        page: {},
    },
} satisfies Meta<typeof DashboardUsage>;

export default meta;

type Story = StoryObj<typeof meta>;

// Assertions about a group's total belong to its whole row, not to the trigger inside it.
function rowFor(trigger: HTMLElement) {
    return trigger.closest("tr")!;
}

// Unpadded base64 of a 16-byte guid: 22 characters, the last carrying only two bits.
const TOPOLOGY_ID = /^[A-Za-z0-9+/]{21}[AQgw]$/;

// The usage column, whatever the row.
function lastCell(row: HTMLElement) {
    const cells = (row as HTMLTableRowElement).cells;
    return cells[cells.length - 1]!;
}

export const Default: Story = {};

export const Empty: Story = {
    parameters: {
        msw: {
            // Overriding a service replaces its whole handler array, so keep the
            // license endpoint and only swap usage for an empty month.
            handlers: {
                settings: [settingsMocks.license(), settingsMocks.usage({ byPeriod: [], perApplication: [] })],
            },
        },
    },
};

// Two appliances under one licence can both run an app called "huetopia", and as bare rows they
// are indistinguishable. They collapse into one counted group, and expanding identifies each by
// topology id - the only thing that tells them apart.
export const SameNamedAppsCollapseIntoOneCountedGroup: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        const huetopia = await waitFor(() => canvas.getByRole("button", { name: "huetopia" }));

        // Collapsed: the group carries the combined usage of its two rows and none of their ids.
        expect(huetopia).toHaveAttribute("aria-expanded", "false");
        expect(rowFor(huetopia)).toHaveTextContent("1,940,000");
        expect(canvas.queryByText(TOPOLOGY_ID)).not.toBeInTheDocument();

        await userEvent.click(huetopia);
        expect(huetopia).toHaveAttribute("aria-expanded", "true");

        // Expanded: one row per database, heaviest first, each named by its id rather than by the
        // name both share.
        const ids = canvas.getAllByText(TOPOLOGY_ID).map((cell) => cell.textContent);
        expect(ids).toEqual([sampleTopologyIds.huetopiaBusiest, sampleTopologyIds.huetopia]);

        await userEvent.click(huetopia);
        expect(canvas.queryByText(TOPOLOGY_ID)).not.toBeInTheDocument();
    },
};

// A uniquely named app has nothing to expand, so it stays the plain row it always was.
export const UniquelyNamedAppStaysAPlainRow: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await waitFor(() => expect(canvas.getByText("support-copilot")).toBeInTheDocument());
        expect(canvas.queryByRole("button", { name: /support-copilot/ })).not.toBeInTheDocument();
    },
};

// The label is a small target in a wide row, so the whole row toggles.
export const ClickingAnywhereInTheRowToggles: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        const trigger = await waitFor(() => canvas.getByRole("button", { name: "huetopia" }));
        const row = rowFor(trigger);

        await userEvent.click(lastCell(row)); // the usage figure, well away from the label
        expect(trigger).toHaveAttribute("aria-expanded", "true");
        expect(canvas.getByText(sampleTopologyIds.huetopiaBusiest)).toBeInTheDocument();
    },
};

// Figures are compared down the column, so their right edges line up whatever their width -
// header included, and including the indented rows of an expanded group.
export const UsageFiguresAreEndAligned: Story = {
    tags: ["!dev"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(await waitFor(() => canvas.getByRole("button", { name: "huetopia" })));

        // Measured rather than asserted against a CSS property: the column has been laid out with
        // text-align and with flex, and what the eye checks is where the digits end.
        const edges = canvas
            .getAllByRole("row")
            .map((row) => Math.round(lastCell(row).firstElementChild!.getBoundingClientRect().right));

        expect(edges.length).toBeGreaterThan(1);
        expect(new Set(edges).size).toBe(1);
    },
};
