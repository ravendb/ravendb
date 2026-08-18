import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, within } from "storybook/test";
import { appsMocks } from "@/mocks/apps-mocks";
import type { CdcLiveRawFrame } from "@/pages/apps/use-cdc-live-performance";
import { AppDataSource } from "./app-data-source";

const meta = {
    title: "Apps/Data Source",
    component: AppDataSource,
    parameters: {
        page: { title: "Data source" },
        // The detail mock only resolves known slugs, so start on a sample app.
        router: { initialPath: "/apps/demo", path: "/apps/:slug" },
    },
} satisfies Meta<typeof AppDataSource>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

// One clean batch, finished well outside the 60s activity window, so the live feed reads
// "Idle" with no errors while the stored log still holds older failures.
function idleCdcProgressFrame(): CdcLiveRawFrame {
    const startedMs = Date.now() - 10 * 60_000;

    return {
        Results: [
            {
                TaskName: "cdc/demo-shop",
                Stats: [
                    {
                        Performance: [
                            {
                                Id: 0,
                                Started: new Date(startedMs).toISOString(),
                                Completed: new Date(startedMs + 1_200).toISOString(),
                                DurationInMs: 1_200,
                                NumberOfReadMessages: 480,
                                NumberOfProcessedMessages: 480,
                                ScriptProcessingErrorCount: 0,
                                ReadErrorCount: 0,
                            },
                        ],
                    },
                ],
            },
        ],
    };
}

// The live window and the stored log genuinely disagree here: nothing is failing right now,
// but earlier syncs did fail. Both counts have to stay on screen naming their own scope,
// otherwise the page contradicts the overview banner (RavenDB-27320).
export const IdleWithRecordedErrors: Story = {
    parameters: {
        msw: {
            handlers: {
                apps: [appsMocks.detail(), appsMocks.cdcProgress(idleCdcProgressFrame()), appsMocks.cdcErrors()],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        // The live feed and the stored log land independently, so each count is awaited.
        expect(await canvas.findByText("Idle")).toBeInTheDocument();
        expect(await canvas.findByText("Recent errors")).toBeInTheDocument();
        expect(await canvas.findByRole("button", { name: /view 3 recorded errors/i })).toBeInTheDocument();
    },
};
