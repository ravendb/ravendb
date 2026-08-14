import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import type { CdcError } from "@/api/generated/server-api";
import { appsMocks, sampleCdcErrors, sampleCdcPerformance } from "@/mocks/apps-mocks";
import { SyncHealthCard } from "./sync-health-card";

// A sink failing right now has just written to its log, so its newest entry is minutes old.
// Reusing the stale sample would claim an active failure whose last error was three weeks ago,
// which cannot happen - and would leave the destructive styling looking arbitrary beside Idle,
// where the very same rows carry no colour at all.
const failingCdcErrors: CdcError[] = sampleCdcErrors.map((error, index) => ({
    ...error,
    createdAt: new Date(Date.now() - (index + 2) * 60_000).toISOString(),
}));

const meta = {
    title: "Apps/Data Sync",
    component: SyncHealthCard,
    args: { slug: "demo" },
    parameters: { page: {} },
} satisfies Meta<typeof SyncHealthCard>;

export default meta;

type Story = StoryObj<typeof meta>;

// The sink is idle while the log still lists older failures. Both readings have to be on screen
// at once, each worded so it cannot be mistaken for the other, and neither styled as an alarm
// (RavenDB-27320).
export const Idle: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        expect(await canvas.findByText("Idle")).toBeInTheDocument();
        expect(await canvas.findByText(/3 recorded, latest 3w ago/i)).toBeInTheDocument();
        expect(await canvas.findByRole("button", { name: /view errors/i })).toBeInTheDocument();

        // Relative on screen, exact on hover, so a lag can be matched against a server log.
        await userEvent.hover(await canvas.findByText("2m ago"));
        // Radix portals the tooltip to the body and keeps a second, visually hidden copy for
        // screen readers, so matching the role alone can land on the one that never renders.
        await waitFor(() => {
            const tooltips = within(document.body).getAllByRole("tooltip");
            expect(tooltips.some((tooltip) => tooltip.checkVisibility())).toBe(true);
        });
    },
};

// An app that never failed must carry no error styling and no errors action.
export const Healthy: Story = {
    parameters: {
        msw: {
            handlers: {
                apps: [appsMocks.cdcPerformance(), appsMocks.cdcErrors([])],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        expect(await canvas.findByText("None recorded")).toBeInTheDocument();
        expect(canvas.queryByRole("button", { name: /view errors/i })).not.toBeInTheDocument();
    },
};

export const Failing: Story = {
    parameters: {
        msw: {
            handlers: {
                apps: [
                    appsMocks.cdcPerformance({ ...sampleCdcPerformance, status: "error", errorCount: 3 }),
                    appsMocks.cdcErrors(failingCdcErrors),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        expect(await canvas.findByText("Error")).toBeInTheDocument();
        // Minutes old, not weeks: the recency is what the destructive styling reacts to.
        expect(await canvas.findByText(/3 recorded, latest 2m ago/i)).toBeInTheDocument();
    },
};

// A sink that cannot reach its source writes process-level errors without ever producing a batch,
// so the rolling window stays empty and the server keeps reporting "idle" with no error count -
// however long it has been failing, and with no exact sync timestamp left to show. The card has to
// call that broken anyway, off the one signal that survives: an error newer than the last sync.
export const FailingWithoutBatches: Story = {
    parameters: {
        msw: {
            handlers: {
                apps: [
                    appsMocks.cdcPerformance({
                        ...sampleCdcPerformance,
                        status: "idle",
                        lastSyncAt: null,
                        lagSeconds: 7 * 24 * 60 * 60,
                    }),
                    appsMocks.cdcErrors(failingCdcErrors),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        expect(await canvas.findByText("1w ago")).toBeInTheDocument();
        expect(await canvas.findByText(/3 recorded, latest 2m ago/i)).toBeInTheDocument();
        // Reported idle, shown as failing.
        expect(await canvas.findByText("Error")).toBeInTheDocument();
    },
};
