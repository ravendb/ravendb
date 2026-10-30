import type { Meta, StoryObj } from "@storybook/react-vite";
import { http, HttpResponse, type HttpHandler } from "msw";
import { expect, userEvent, waitFor, within } from "storybook/test";
import type { MigrationPlanSaveRequest } from "@/api/generated/server-api";
import { assistantMocks } from "@/mocks/assistant-mocks";
import { consentHandlers, defaultApiMocks, discoverHandlers, mockHandlers } from "@/mocks/default-mocks";
import {
    LARGE_PLAN_AREAS,
    largePlanAreaBuildFrames,
    largePlanDiscovery,
    largePlanOpeningFrames,
    largePlanSelectedTables,
} from "@/mocks/migration-large-plan-mocks";
import {
    migrationMocks,
    PLANNER_DEFAULT_SCHEMA,
    plannerAnswers,
    plannerAnswersFrames,
    plannerBuildFrames,
    plannerCustomersBuildFrames,
    plannerDiscovery,
    plannerAreaBuildFrames,
    plannerOpeningFrames,
    plannerRetryFrames,
    PLANNER_RETRY_PROMPT,
    samplePlanView,
} from "@/mocks/migration-mocks";
import { setupMocks } from "@/mocks/setup-mocks";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { AppWizardAtStep } from "@/pages/setup/add-app-wizard/stories/app-wizard-at-step";
import { replayPlannerTurns, type PlannerScriptTurn } from "@/pages/setup/add-app-wizard/stories/app-wizard-fixtures";
import type { PlannerView } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

const meta = {
    title: "Setup/Add App Wizard/Plan Document Model",
    parameters: {
        page: { bare: true },
        msw: mockHandlers(discoverHandlers(plannerDiscovery)),
    },
} satisfies Meta;

export default meta;

type Story = StoryObj<typeof meta>;

const SALES = "Sales area";

const CUSTOMERS = "Customers area";

const OPENING: PlannerScriptTurn[] = [{ frames: plannerOpeningFrames }];

const ANSWERED: PlannerScriptTurn[] = [
    ...OPENING,
    {
        userTurn: { kind: "answers", id: "answers", answers: plannerAnswers },
        frames: plannerAnswersFrames,
    },
];

const SALES_BUILT: PlannerScriptTurn[] = [...ANSWERED, { area: SALES, frames: plannerBuildFrames }];

const ALL_BUILT: PlannerScriptTurn[] = [
    ...SALES_BUILT,
    { area: CUSTOMERS, frames: plannerCustomersBuildFrames },
    {
        area: SALES,
        userTurn: { kind: "prompt", id: "retry", text: PLANNER_RETRY_PROMPT, mentions: [] },
        frames: plannerRetryFrames,
    },
];

/**
 * Every planner story runs the real wizard at its step, on the demo_shop sample the designs use: a SQL
 * Server database, so a table named without a schema is in dbo. The form holds no mapping yet.
 */
function PlannerAtStep({
    turns,
    view,
    seedStore,
}: {
    turns?: PlannerScriptTurn[];
    view?: PlannerView;
    seedStore?: () => void;
}) {
    return (
        <AppWizardAtStep
            initialStep="map"
            discovery={plannerDiscovery}
            seedOverride={(seed) => ({
                ...seed,
                externalConnection: { ...seed.externalConnection, provider: "SqlClient" },
                mapTables: { tables: [] },
            })}
            seedStore={(seed) => {
                useSetupWizardStore.setState({ mapOrigin: null });

                if (turns) {
                    replaySession(turns, seed);
                }

                if (view) {
                    useSetupWizardStore.getState().showPlannerView(view);
                }

                seedStore?.();
            }}
        />
    );
}

function replaySession(turns: PlannerScriptTurn[], seed: AppFormData) {
    replayPlannerTurns(turns, {
        slug: seed.externalConnection.slug,
        selectedTables: seed.verifySchema.tables,
        defaultSchema: PLANNER_DEFAULT_SCHEMA,
    });
}

function setupHandlers(...handlers: HttpHandler[]) {
    return { setup: [setupMocks.discover(plannerDiscovery), ...handlers, ...defaultApiMocks.setup] };
}

const fast = { frameDelayMs: 20 };

// A new operator accepts the AI terms once; the button then starts the planner.
export const TermsRequired: Story = {
    parameters: { msw: mockHandlers(consentHandlers("ConsentRequired")) },
    render: () => <PlannerAtStep />,
};

// Every first visit starts here. The terms were accepted before, so they are not shown.
export const Welcome: Story = {
    render: () => <PlannerAtStep />,
};

// The AI service is out of reach, so manual mapping is the only way on.
export const AiUnavailable: Story = {
    parameters: {
        msw: mockHandlers({ assistant: [assistantMocks.consentUnavailable(), ...defaultApiMocks.assistant] }),
    },
    render: () => <PlannerAtStep />,
};

// The planner reads the schema and proposes the areas.
export const Proposing: Story = {
    parameters: { msw: mockHandlers(setupHandlers(migrationMocks.proposalStart([], { isEndless: true }))) },
    render: () => <PlannerAtStep turns={[{ frames: [], isWorking: true }]} />,
};

// Its questions settle the conventions before any area starts; Build waits for the answers.
export const Questions: Story = {
    render: () => <PlannerAtStep turns={OPENING} />,
};

// With the answers in, the plan is ready: each area builds on its own, from its card.
export const PlanReady: Story = {
    render: () => (
        <PlannerAtStep
            turns={ANSWERED}
            seedStore={() => useSetupWizardStore.getState().setPlannerExpandedAreas([SALES])}
        />
    ),
};

// An area's agent at work: its cards fill in as each collection registers, and other areas stay open.
export const AreaBuilding: Story = {
    render: () => (
        <PlannerAtStep
            turns={[...ANSWERED, { area: SALES, frames: plannerBuildFrames.slice(0, 3), isWorking: true }]}
            view={{ area: SALES, collection: null }}
        />
    ),
};

// What the area's agent built, what it rejected and why, and one-click replies to carry on.
export const AreaReview: Story = {
    render: () => <PlannerAtStep turns={SALES_BUILT} view={{ area: SALES, collection: null }} />,
};

// The plan once an area started: the property names are locked, and the strips map the migration.
export const PlanInProgress: Story = {
    render: () => <PlannerAtStep turns={SALES_BUILT} />,
};

// A mapped collection's page, its mapping edited with the Map schema editor.
export const CollectionPage: Story = {
    render: () => <PlannerAtStep turns={ALL_BUILT} view={{ area: SALES, collection: "Orders" }} />,
};

// A rejected collection opens with its problems, ready to be fixed by hand or by its agent.
export const RejectedCollectionPage: Story = {
    render: () => <PlannerAtStep turns={SALES_BUILT} view={{ area: SALES, collection: "Products" }} />,
};

// Manual changes wait on the unsaved-changes card, on every view, until saved or discarded.
export const UnsavedChanges: Story = {
    render: () => (
        <PlannerAtStep
            turns={ALL_BUILT}
            view={{ area: SALES, collection: null }}
            seedStore={() =>
                useSetupWizardStore
                    .getState()
                    .stagePlannerEdit("Products", { area: SALES, config: null, isRemoved: true })
            }
        />
    ),
};

// A 200-table plan, a few areas built.
export const LargePlan: Story = {
    parameters: { msw: mockHandlers(discoverHandlers(largePlanDiscovery)) },
    render: () => (
        <AppWizardAtStep
            initialStep="map"
            discovery={largePlanDiscovery}
            seedOverride={(seed) => ({
                ...seed,
                externalConnection: { ...seed.externalConnection, provider: "SqlClient" },
                verifySchema: { tables: largePlanSelectedTables },
                mapTables: { tables: [] },
            })}
            seedStore={(seed) => {
                useSetupWizardStore.setState({ mapOrigin: null });
                replaySession(
                    [
                        { frames: largePlanOpeningFrames },
                        ...LARGE_PLAN_AREAS.slice(0, 3).map((area) => ({
                            area,
                            frames: largePlanAreaBuildFrames(area),
                        })),
                    ],
                    seed,
                );
            }}
        />
    ),
};

// Accepting the terms starts the planner, which proposes and then asks its questions.
export const StartPlanner: Story = {
    tags: ["test-only"],
    parameters: { msw: mockHandlers(setupHandlers(migrationMocks.proposalStart(plannerOpeningFrames, fast))) },
    render: () => <PlannerAtStep />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(await canvas.findByRole("button", { name: "Start the planner" }));

        // The plan's status line and the questions card in the chat both say so.
        await waitFor(() => expect(canvas.getAllByText("The planner has 3 questions")).toHaveLength(2));
        const customers = within(canvas.getByRole("region", { name: CUSTOMERS }));
        expect(customers.getByRole("button", { name: "Build" })).toHaveAttribute("aria-disabled", "true");
    },
};

// Number keys pick an option; the last question sends every answer, and the plan is ready to build.
export const AnswerQuestions: Story = {
    tags: ["test-only"],
    parameters: { msw: mockHandlers(setupHandlers(migrationMocks.proposalAsk(plannerAnswersFrames, fast))) },
    render: () => <PlannerAtStep turns={OPENING} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("radio", { name: /link to products by id/i }));
        await userEvent.keyboard("2");
        expect(canvas.getByRole("radio", { name: /copy the product name/i })).toBeChecked();

        await userEvent.click(canvas.getByRole("button", { name: "Next" }));
        await userEvent.click(canvas.getByRole("button", { name: "Next" }));
        await userEvent.click(canvas.getByRole("button", { name: "Send answers" }));

        await waitFor(() => expect(canvas.getByText("The plan is ready")).toBeInTheDocument());
        expect(canvas.getByText("Copy the product name and price into each line")).toBeInTheDocument();
        expect(canvas.getByRole("button", { name: /property names camelCase/i })).toBeInTheDocument();
    },
};

// Closing the questions leaves a way back to them, and a message sent instead drops them.
export const WriteInsteadOfAnswering: Story = {
    tags: ["test-only"],
    parameters: { msw: mockHandlers(setupHandlers(migrationMocks.proposalAsk(plannerAnswersFrames, fast))) },
    render: () => <PlannerAtStep turns={OPENING} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("button", { name: "Close, and write a message instead" }));
        const composer = canvas.getByRole("textbox", { name: "Message for the planner" });
        await waitFor(() => expect(composer).toHaveFocus());
        expect(canvas.getByText("The planner still has 3 questions.")).toBeInTheDocument();

        await userEvent.click(canvas.getByRole("button", { name: "Answer them" }));
        expect(canvas.getByRole("radio", { name: /link to products by id/i })).toBeInTheDocument();

        await userEvent.click(canvas.getByRole("button", { name: "Close, and write a message instead" }));
        await userEvent.type(canvas.getByRole("textbox", { name: "Message for the planner" }), "Link everything.");
        await userEvent.click(canvas.getByRole("button", { name: "Send" }));

        await waitFor(() => expect(canvas.getByText("The plan is ready")).toBeInTheDocument());
        expect(canvas.queryByText("The planner still has 3 questions.")).not.toBeInTheDocument();
    },
};

// Build starts the area's agent and opens the area, where its cards fill in; the conventions lock.
export const BuildArea: Story = {
    tags: ["test-only"],
    parameters: {
        msw: mockHandlers(setupHandlers(migrationMocks.areaStart(({ area }) => plannerAreaBuildFrames(area), fast))),
    },
    render: () => <PlannerAtStep turns={ANSWERED} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(
            within(canvas.getByRole("region", { name: SALES })).getByRole("button", { name: "Build" }),
        );

        await waitFor(() => expect(canvas.getByRole("heading", { level: 2, name: SALES })).toBeVisible());
        expect(await canvas.findByRole("button", { name: "Retry Products" })).toBeInTheDocument();
        expect(canvas.getByRole("button", { name: "Ask the planner to fix it" })).toBeInTheDocument();
        expect(useSetupWizardStore.getState().plannerStartedAreas).toEqual([SALES]);
    },
};

const savedPlans: MigrationPlanSaveRequest[] = [];

const recordPlanSave = http.post("/api/setup/migration/plan/save", async ({ request }) => {
    savedPlans.push((await request.json()) as MigrationPlanSaveRequest);
    return HttpResponse.json({ issues: [], warnings: [] });
});

// A property renamed in the mapping waits on the card; Save plan sends it as an edit.
export const EditAndSave: Story = {
    tags: ["test-only"],
    parameters: { msw: mockHandlers(setupHandlers(recordPlanSave, migrationMocks.planGet(samplePlanView))) },
    render: () => <PlannerAtStep turns={ALL_BUILT} view={{ area: SALES, collection: "Orders" }} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        savedPlans.length = 0;

        const property = canvas.getByDisplayValue("placedAt");
        await userEvent.clear(property);
        await userEvent.type(property, "orderedAt");

        await waitFor(() => expect(canvas.getByText("You have 1 unsaved change")).toBeInTheDocument());
        await userEvent.click(canvas.getByRole("button", { name: "Save plan" }));

        await waitFor(() => expect(savedPlans).toHaveLength(1));
        expect(savedPlans[0].changes).toEqual([
            expect.objectContaining({ kind: "Edit", area: SALES, collection: "Orders" }),
        ]);
        expect(JSON.stringify(savedPlans[0].changes[0].config)).toContain("orderedAt");
    },
};

// Removing a card waits on the card too, and goes out as a delete.
export const RemoveCollection: Story = {
    tags: ["test-only"],
    parameters: { msw: mockHandlers(setupHandlers(recordPlanSave, migrationMocks.planGet(samplePlanView))) },
    render: () => <PlannerAtStep turns={ALL_BUILT} view={{ area: SALES, collection: null }} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        savedPlans.length = 0;

        const [removeProducts] = canvas.getAllByRole("button", { name: "Remove" }).slice(-1);
        await userEvent.click(removeProducts);

        await waitFor(() => expect(canvas.getByText("Removing, unsaved")).toBeInTheDocument());
        await userEvent.click(canvas.getByRole("button", { name: "Save plan" }));

        await waitFor(() => expect(savedPlans).toHaveLength(1));
        expect(savedPlans[0].changes).toEqual([{ kind: "Delete", area: SALES, collection: "Products" }]);
    },
};

const stoppedTurns: unknown[] = [];

// Stop ends the area's turn on the server, not only its stream.
export const StopArea: Story = {
    tags: ["test-only"],
    parameters: {
        msw: mockHandlers(
            setupHandlers(
                migrationMocks.areaStart(plannerBuildFrames.slice(0, 1), { isEndless: true }),
                http.post("/api/setup/migration/stop", async ({ request }) => {
                    stoppedTurns.push(await request.json());
                    return new HttpResponse(null, { status: 204 });
                }),
            ),
        ),
    },
    render: () => <PlannerAtStep turns={ANSWERED} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        stoppedTurns.length = 0;

        await userEvent.click(
            within(canvas.getByRole("region", { name: SALES })).getByRole("button", { name: "Build" }),
        );
        await userEvent.click(await canvas.findByRole("button", { name: `Stop the ${SALES} agent` }));

        await waitFor(() => expect(stoppedTurns).toEqual([{ slug: "acme-shop", agent: "Area", area: SALES }]));
    },
};

// The plan is the last step: creating the app applies it first.
export const CreateApp: Story = {
    tags: ["test-only"],
    render: () => <PlannerAtStep turns={ALL_BUILT} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        expect(canvas.queryByRole("button", { name: /^next/i })).not.toBeInTheDocument();
        // Said on the plan's status line, and beside the button that creates the app.
        expect(canvas.getAllByText("3 collections go to the app.")).toHaveLength(2);

        await userEvent.click(canvas.getByRole("button", { name: /create app & continue/i }));

        await waitFor(() => expect(useSetupWizardStore.getState().mapOrigin).toMatchObject({ source: "planner" }));
    },
};

// Collections not built yet are left out of the app, so creating it asks first.
export const CreateAppWithUnbuiltCollections: Story = {
    tags: ["test-only"],
    render: () => <PlannerAtStep turns={SALES_BUILT} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(canvasElement.ownerDocument.body);
        const createApp = canvas.getByRole("button", { name: /create app & continue/i });
        const dialogName = "Create the app without all collections?";

        await userEvent.click(createApp);
        await userEvent.click(
            within(await body.findByRole("alertdialog", { name: dialogName })).getByRole("button", { name: "Cancel" }),
        );
        await waitFor(() => expect(body.queryByRole("alertdialog")).not.toBeInTheDocument());
        expect(useSetupWizardStore.getState().mapOrigin).toBeNull();

        await userEvent.click(createApp);
        await userEvent.click(
            within(await body.findByRole("alertdialog", { name: dialogName })).getByRole("button", {
                name: "Create app",
            }),
        );

        await waitFor(() => expect(useSetupWizardStore.getState().mapOrigin).toMatchObject({ source: "planner" }));
    },
};

// A plan that does not validate as a whole keeps the wizard on the planner, with the reasons.
export const ApplyRejected: Story = {
    tags: ["test-only"],
    parameters: {
        msw: mockHandlers(
            setupHandlers(migrationMocks.applyRejected(["Customers and Orders both map dbo.Customers.Name to name."])),
        ),
    },
    render: () => <PlannerAtStep turns={ALL_BUILT} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("button", { name: /create app & continue/i }));

        await waitFor(() => expect(canvas.getByText("The mapping did not pass validation.")).toBeInTheDocument());
        expect(canvas.getByRole("heading", { level: 1, name: "Plan your document model" })).toBeInTheDocument();
    },
};

// Manual mapping leaves the planner for the Map schema step, which joins the flow as its last step.
export const UseManualMapping: Story = {
    tags: ["test-only"],
    render: () => <PlannerAtStep />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(await canvas.findByRole("button", { name: "use manual mapping" }));

        await waitFor(() => expect(canvas.getByRole("heading", { level: 1, name: "Map schema" })).toBeInTheDocument());
        expect(canvas.getByRole("button", { name: /create app & continue/i })).toBeInTheDocument();
    },
};

// Starting over asks first, then clears the plan and goes back to the welcome view.
export const StartOver: Story = {
    tags: ["test-only"],
    render: () => <PlannerAtStep turns={SALES_BUILT} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(canvasElement.ownerDocument.body);

        await userEvent.click(canvas.getByRole("button", { name: "Start over" }));
        const dialog = await body.findByRole("alertdialog", { name: "Start over?" });
        await userEvent.click(within(dialog).getByRole("button", { name: "Start over" }));

        await waitFor(() => expect(canvas.getByRole("button", { name: "Start the planner" })).toBeInTheDocument());
        expect(useSetupWizardStore.getState().plannerEntries).toEqual({});
    },
};
