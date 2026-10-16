import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { assistantMocks } from "@/mocks/assistant-mocks";
import { consentHandlers, defaultApiMocks, discoverHandlers } from "@/mocks/default-mocks";
import {
    LARGE_PLAN_CONVERSATION_ID,
    largePlanBuildFrames,
    largePlanDiscovery,
    largePlanOpeningFrames,
    largePlanSelectedTables,
} from "@/mocks/migration-large-plan-mocks";
import {
    loanTablesDiscovery,
    migrationMocks,
    plannerAnswers,
    plannerBuildFrames,
    plannerDiscovery,
    plannerOpeningFrames,
    plannerRetryFrames,
    PLANNER_RETRY_PROMPT,
} from "@/mocks/migration-mocks";
import { setupMocks } from "@/mocks/setup-mocks";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { AppWizardAtStep } from "@/pages/setup/add-app-wizard/stories/app-wizard-at-step";
import { replayPlannerTurns, type PlannerScriptTurn } from "@/pages/setup/add-app-wizard/stories/app-wizard-fixtures";

const meta = {
    title: "Setup/Add App Wizard/Plan Document Model",
    parameters: {
        page: { bare: true },
        msw: { handlers: discoverHandlers(plannerDiscovery) },
    },
} satisfies Meta;

export default meta;

type Story = StoryObj<typeof meta>;

const OPENING: PlannerScriptTurn[] = [{ frames: plannerOpeningFrames }];

const ANSWERS: PlannerScriptTurn["userTurn"] = {
    kind: "answers",
    id: "answers",
    answers: plannerAnswers,
    isSkipped: false,
    instructions: null,
};

const OPENING_WITHOUT_QUESTIONS: PlannerScriptTurn[] = [
    {
        frames: plannerOpeningFrames.map((frame) => (frame.type === "reply" ? { ...frame, openQuestions: [] } : frame)),
    },
];

const REVIEW: PlannerScriptTurn[] = [...OPENING, { userTurn: ANSWERS, frames: plannerBuildFrames }];

const RETRY: PlannerScriptTurn[] = [
    { userTurn: { kind: "prompt", id: "retry", text: PLANNER_RETRY_PROMPT, mentions: [] }, frames: plannerRetryFrames },
];

const LARGE_PLAN: PlannerScriptTurn[] = [
    { frames: largePlanOpeningFrames },
    {
        userTurn: { kind: "prompt", id: "build", text: "Build the plan.", mentions: [] },
        frames: largePlanBuildFrames,
    },
];

/**
 * Every planner story runs the real wizard at its step, on the demo_shop sample the designs use. The
 * form holds no mapping yet, as on a new app, unless the story says otherwise.
 */
function PlannerAtStep({
    seedStore,
    hasMapping = false,
}: {
    seedStore?: (seed: AppFormData) => void;
    hasMapping?: boolean;
}) {
    return (
        <AppWizardAtStep
            initialStep="map"
            discovery={plannerDiscovery}
            seedOverride={hasMapping ? undefined : (seed) => ({ ...seed, mapTables: { tables: [] } })}
            seedStore={seedStore}
        />
    );
}

const replay = (turns: PlannerScriptTurn[]) => (seed: AppFormData) =>
    replayPlannerTurns(turns, seed.verifySchema.tables);

function startHandlers(start: ReturnType<typeof migrationMocks.start>) {
    return { setup: [setupMocks.discover(plannerDiscovery), start, ...defaultApiMocks.setup] };
}

// C0 - A new operator accepts the AI terms once, ticking them as everywhere else; accepting starts the planner.
export const TermsRequired: Story = {
    parameters: { msw: { handlers: consentHandlers("ConsentRequired") } },
    render: () => <PlannerAtStep />,
};

// The AI service is out of reach, so manual mapping is the only way on.
export const AiUnavailable: Story = {
    parameters: {
        msw: { handlers: { assistant: [assistantMocks.consentUnavailable(), ...defaultApiMocks.assistant] } },
    },
    render: () => <PlannerAtStep />,
};

// C1 - With the terms accepted, the planner starts as soon as the step opens.
export const Starting: Story = {
    parameters: { msw: { handlers: startHandlers(migrationMocks.start([], { isEndless: true })) } },
    render: () => <PlannerAtStep />,
};

// C1b - Past eight tables the chips fold behind "+N more", and a shared schema moves into the label.
export const StartingWithManyTables: Story = {
    parameters: {
        msw: {
            handlers: {
                setup: [
                    setupMocks.discover(loanTablesDiscovery),
                    migrationMocks.start([], { isEndless: true }),
                    ...defaultApiMocks.setup,
                ],
            },
        },
    },
    render: () => (
        <AppWizardAtStep
            initialStep="map"
            discovery={loanTablesDiscovery}
            seedOverride={(seed) => ({ ...seed, mapTables: { tables: [] } })}
        />
    ),
};

// C2 - The planner's questions take the composer's place, the recommended answer preselected.
export const Questions: Story = {
    render: () => <PlannerAtStep seedStore={replay(OPENING)} />,
};

// What the planner noted while opening, and a connection that dropped after it finished, show once it is done.
export const OpeningActivity: Story = {
    render: () => (
        <PlannerAtStep
            seedStore={replay([
                {
                    frames: [
                        { type: "note", text: "Left dbo.AuditLog out: nothing in the app reads it" },
                        ...plannerOpeningFrames,
                        { type: "error", message: "The AI service could not be reached." },
                    ],
                },
            ])}
        />
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        expect(canvas.getByText("Left dbo.AuditLog out: nothing in the app reads it")).toBeInTheDocument();
        expect(
            canvas.getByText("The connection dropped after the planner finished: The AI service could not be reached."),
        ).toBeInTheDocument();
    },
};

// C2b - With nothing to ask, the planner's proposal ends on the next step: building the suggested collections.
export const Proposal: Story = {
    render: () => <PlannerAtStep seedStore={replay(OPENING_WITHOUT_QUESTIONS)} />,
};

// C3 - The planner at work after the answers: the turn's activity grows as collections are built.
export const Building: Story = {
    render: () => (
        <PlannerAtStep
            seedStore={replay([
                ...OPENING,
                { userTurn: ANSWERS, frames: plannerBuildFrames.slice(0, 2), isWorking: true },
            ])}
        />
    ),
};

// C4 - What was built, what failed, and one-click replies to carry on.
export const Review: Story = {
    render: () => <PlannerAtStep seedStore={replay(REVIEW)} />,
};

// C5 - The plan panel closed; the Plan button flags the collections that changed since.
export const PlanClosed: Story = {
    render: () => (
        <PlannerAtStep
            seedStore={(seed) => {
                replay(REVIEW)(seed);
                useSetupWizardStore.getState().closePlannerPanel();
                replay(RETRY)(seed);
            }}
        />
    ),
};

// C6 - A collection's details in the panel, with the collection mentioned in the composer.
export const CollectionDetails: Story = {
    render: () => (
        <PlannerAtStep
            seedStore={(seed) => {
                replay([...REVIEW, ...RETRY])(seed);
                useSetupWizardStore.getState().openPlannerPanel("Orders");
                useSetupWizardStore.getState().addPlannerMention("Orders");
            }}
        />
    ),
};

// C7 - A 200-table plan, filtered by name and state.
export const LargePlan: Story = {
    parameters: {
        msw: { handlers: { setup: [setupMocks.discover(largePlanDiscovery), ...defaultApiMocks.setup] } },
    },
    render: () => (
        <AppWizardAtStep
            initialStep="map"
            discovery={largePlanDiscovery}
            seedOverride={(seed) => ({
                ...seed,
                verifySchema: { tables: largePlanSelectedTables },
                mapTables: { tables: [] },
            })}
            seedStore={() => replayPlannerTurns(LARGE_PLAN, largePlanSelectedTables)}
        />
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.type(canvas.getByRole("searchbox", { name: /filter the plan/i }), "order");

        await waitFor(() => expect(canvas.getByText(/collections shown/)).toBeInTheDocument());
    },
};

// C8 - Every table and where it goes; tables the planner left out can be handed back to it.
export const LargePlanTables: Story = {
    parameters: LargePlan.parameters,
    render: LargePlan.render,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("tab", { name: /tables/i }));
        await userEvent.type(canvas.getByRole("searchbox", { name: /filter the tables/i }), "product");
        await userEvent.click(canvas.getByRole("checkbox", { name: "Choose report.TopProducts to add to the plan" }));

        expect(canvas.getByRole("button", { name: "Ask the planner to add it" })).toBeInTheDocument();
        expect(useSetupWizardStore.getState().plannerConversationId).toBe(LARGE_PLAN_CONVERSATION_ID);
    },
};

// The form already holds a mapping - imported, or kept from an earlier visit - so the planner waits to be
// asked, and Next carries the mapping on.
export const MappingInForm: Story = {
    render: () => <PlannerAtStep hasMapping />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await waitFor(() => expect(canvas.getByRole("button", { name: "Start the planner" })).toBeInTheDocument());
        expect(canvas.getByText("Next keeps the current mapping.")).toBeInTheDocument();
        expect(canvas.getByRole("button", { name: "Next" })).toBeEnabled();
        expect(useSetupWizardStore.getState().plannerTurns).toEqual([]);
    },
};

// Start over asks before clearing the conversation and the plan.
export const StartOverDialog: Story = {
    render: Review.render,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(canvasElement.ownerDocument.body);

        await userEvent.click(canvas.getByRole("button", { name: "Start over" }));

        await waitFor(() => expect(body.getByRole("alertdialog", { name: "Start over?" })).toBeInTheDocument());
    },
};

/*
   The stories below drive the planner's transitions. They end up looking like a board already on show
   above, so they are tagged "!dev" to keep them out of the sidebar; the vitest addon still runs them.
*/

const fastHandlers = {
    assistant: [
        assistantMocks.consent({ status: "ConsentRequired" }),
        assistantMocks.giveConsent({ status: "Success" }),
        ...defaultApiMocks.assistant,
    ],
    setup: [
        setupMocks.discover(plannerDiscovery),
        migrationMocks.start(plannerOpeningFrames, { frameDelayMs: 20 }),
        migrationMocks.ask(plannerBuildFrames, { frameDelayMs: 20 }),
        ...defaultApiMocks.setup,
    ],
};

// Accepting the terms starts the planner, which opens with its questions.
export const AcceptTermsAndStart: Story = {
    tags: ["!dev"],
    parameters: { msw: { handlers: fastHandlers } },
    render: () => <PlannerAtStep />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const accept = await canvas.findByRole("button", { name: /accept and start/i });

        expect(accept).toBeDisabled();
        await userEvent.click(canvas.getByRole("checkbox", { name: /terms of use/i }));
        await userEvent.click(accept);

        await waitFor(() => expect(canvas.getByText("The planner has 3 questions")).toBeInTheDocument());
        expect(canvas.getByRole("button", { name: /apply to mapping/i })).toBeDisabled();
    },
};

// Skipping answers with the recommendations, whatever was picked meanwhile.
export const SkipQuestions: Story = {
    tags: ["!dev"],
    parameters: { msw: { handlers: { setup: fastHandlers.setup } } },
    render: () => <PlannerAtStep seedStore={replay(OPENING)} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("radio", { name: /copy the product name/i }));
        await userEvent.click(canvas.getByRole("button", { name: "Skip, use recommended answers" }));

        await waitFor(() =>
            expect(
                canvas.getByText("You skipped the questions, so the planner goes with its recommendations"),
            ).toBeInTheDocument(),
        );
        expect(canvas.getByText("Link to Products by id")).toBeInTheDocument();
        expect(canvas.queryByText("Copy the product name and price into each line")).not.toBeInTheDocument();
    },
};

// Tabbing into "Something else" keeps the option picked; typing there is what picks it.
export const TabPastCustomAnswer: Story = {
    tags: ["!dev"],
    render: () => <PlannerAtStep seedStore={replay(OPENING)} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const recommended = canvas.getByRole("radio", { name: /link to products by id/i });

        recommended.focus();
        await userEvent.tab();

        expect(canvas.getByRole("textbox", { name: "Something else" })).toHaveFocus();
        expect(recommended).toBeChecked();
        expect(canvas.getByRole("button", { name: "Next" })).toBeEnabled();

        await userEvent.keyboard("Embed the product");
        expect(canvas.getByRole("radio", { name: "Something else" })).toBeChecked();
    },
};

// Picking "Something else" asks for nothing yet; moving on with it still empty is what does.
export const EmptyCustomAnswer: Story = {
    tags: ["!dev"],
    render: () => <PlannerAtStep seedStore={replay(OPENING)} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const customAnswer = canvas.getByRole("textbox", { name: "Something else" });
        const error = "Pick an answer, or write your own.";

        await userEvent.click(canvas.getByRole("radio", { name: "Something else" }));
        expect(canvas.queryByText(error)).not.toBeInTheDocument();

        await userEvent.click(canvas.getByRole("button", { name: "Next" }));
        await waitFor(() => expect(canvas.getByText(error)).toBeInTheDocument());
        expect(customAnswer).toHaveFocus();
        expect(canvas.getByText("1 of 3")).toBeInTheDocument();

        await userEvent.type(customAnswer, "Embed the product");
        await waitFor(() => expect(canvas.queryByText(error)).not.toBeInTheDocument());

        await userEvent.click(canvas.getByRole("button", { name: "Next" }));
        await waitFor(() => expect(canvas.getByText("2 of 3")).toBeInTheDocument());
    },
};

// Number keys pick an option; the last question sends every answer and the planner builds from them.
export const AnswerQuestions: Story = {
    tags: ["!dev"],
    parameters: { msw: { handlers: { setup: fastHandlers.setup } } },
    render: () => <PlannerAtStep seedStore={replay(OPENING)} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("radio", { name: /link to products by id/i }));
        await userEvent.keyboard("2");
        expect(canvas.getByRole("radio", { name: /copy the product name/i })).toBeChecked();

        await userEvent.click(canvas.getByRole("button", { name: "Next" }));
        await userEvent.click(canvas.getByRole("button", { name: "Next" }));
        await userEvent.click(canvas.getByRole("button", { name: "Send answers" }));

        await waitFor(() => expect(canvas.getByText("Built 2 of 3 collections")).toBeInTheDocument());
        expect(canvas.getByText("Copy the product name and price into each line")).toBeInTheDocument();
        expect(canvas.getByText("2 collections go to the mapping, 1 unbuilt is left out.")).toBeInTheDocument();
        expect(canvas.getByRole("button", { name: "Retry Products" })).toBeInTheDocument();
    },
};

// With no questions to answer, the whole proposal is built with one click.
export const BuildSuggested: Story = {
    tags: ["!dev"],
    parameters: {
        msw: {
            handlers: {
                setup: [
                    setupMocks.discover(plannerDiscovery),
                    migrationMocks.ask(plannerBuildFrames, { frameDelayMs: 20 }),
                    ...defaultApiMocks.setup,
                ],
            },
        },
    },
    render: () => <PlannerAtStep seedStore={replay(OPENING_WITHOUT_QUESTIONS)} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        const build = canvas.getByRole("button", { name: "Build suggested collections" });
        await waitFor(() => expect(build).toBeEnabled());
        await userEvent.click(build);

        await waitFor(() => expect(canvas.getByText("Built 2 of 3 collections")).toBeInTheDocument());
        expect(canvas.queryByRole("button", { name: "Build suggested collections" })).not.toBeInTheDocument();
    },
};

// Every removal asks first; nothing links to Orders, so there is no warning about links.
export const RemoveCollection: Story = {
    tags: ["!dev"],
    render: () => <PlannerAtStep seedStore={replay(REVIEW)} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(canvasElement.ownerDocument.body);

        await userEvent.click(canvas.getByRole("button", { name: "Remove Orders from the plan" }));
        const dialog = await body.findByRole("alertdialog", { name: "Remove Orders from the plan?" });
        expect(within(dialog).getByText("Orders won't go to the mapping.")).toBeInTheDocument();
        await userEvent.click(within(dialog).getByRole("button", { name: "Remove" }));

        await waitFor(() =>
            expect(canvas.getByText("1 collection goes to the mapping, 1 unbuilt is left out.")).toBeInTheDocument(),
        );
    },
};

// Removing a collection another one links to asks first; applying hands the built collections to the editor.
export const ApplyToMapping: Story = {
    tags: ["!dev"],
    render: () => <PlannerAtStep seedStore={replay(REVIEW)} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(canvasElement.ownerDocument.body);

        await userEvent.click(canvas.getByRole("button", { name: "Remove Customers from the plan" }));
        const dialog = await body.findByRole("alertdialog", { name: "Remove Customers from the plan?" });
        expect(within(dialog).getByText(/^Orders links to Customers\./)).toBeInTheDocument();
        await userEvent.click(within(dialog).getByRole("button", { name: "Remove" }));

        await waitFor(() =>
            expect(canvas.getByText("1 collection goes to the mapping, 1 unbuilt is left out.")).toBeInTheDocument(),
        );

        await userEvent.click(canvas.getByRole("button", { name: /apply to mapping/i }));

        await waitFor(() => expect(canvas.getByRole("heading", { level: 1, name: "Map schema" })).toBeInTheDocument());
    },
};

// A plan that does not validate as a whole keeps the wizard on the planner, with the reasons.
export const ApplyRejected: Story = {
    tags: ["!dev"],
    parameters: {
        msw: {
            handlers: {
                setup: [
                    setupMocks.discover(plannerDiscovery),
                    migrationMocks.applyRejected(["Customers and Orders both map dbo.Customers.Name to name."]),
                    ...defaultApiMocks.setup,
                ],
            },
        },
    },
    render: () => <PlannerAtStep seedStore={replay(REVIEW)} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("button", { name: /apply to mapping/i }));

        await waitFor(() =>
            expect(canvas.getByText("The registered mapping did not pass validation.")).toBeInTheDocument(),
        );
        expect(canvas.getByRole("heading", { level: 1, name: "Plan your document model" })).toBeInTheDocument();
    },
};

// Manual mapping leaves the planner for the editor straight away.
export const UseManualMapping: Story = {
    tags: ["!dev"],
    render: () => <PlannerAtStep seedStore={replay(OPENING)} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("button", { name: /use manual mapping/i }));

        await waitFor(() => expect(canvas.getByRole("heading", { level: 1, name: "Map schema" })).toBeInTheDocument());
    },
};

// The tables the planner reads can be changed from its first step.
export const ChangeSelection: Story = {
    tags: ["!dev"],
    parameters: Starting.parameters,
    render: () => <PlannerAtStep />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(await canvas.findByRole("button", { name: "Change selection" }));

        await waitFor(() =>
            expect(canvas.getByRole("heading", { level: 1, name: "Verify your schema" })).toBeInTheDocument(),
        );
    },
};

// Instructions typed while the first plan is made wait for its questions, and go in with the answers.
export const InstructionsWhileStarting: Story = {
    tags: ["!dev"],
    parameters: {
        msw: {
            handlers: {
                setup: [
                    setupMocks.discover(plannerDiscovery),
                    migrationMocks.start(plannerOpeningFrames, { frameDelayMs: 300 }),
                    migrationMocks.ask(plannerBuildFrames, { frameDelayMs: 20 }),
                    ...defaultApiMocks.setup,
                ],
            },
        },
    },
    render: () => <PlannerAtStep />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.type(
            await canvas.findByRole("textbox", { name: "Instructions for the planner" }),
            "Orders are always read with their lines.",
        );

        await waitFor(() => expect(canvas.getByText("The planner has 3 questions")).toBeInTheDocument(), {
            timeout: 5000,
        });
        expect(canvas.getByText(/Your note goes with the answers/)).toBeInTheDocument();

        await userEvent.click(canvas.getByRole("button", { name: "Skip, use recommended answers" }));

        await waitFor(() => expect(canvas.getByText("Orders are always read with their lines.")).toBeInTheDocument());
        await waitFor(() => expect(canvas.getByText("Built 2 of 3 collections")).toBeInTheDocument());
    },
};

// A collection mentioned from its details leads the next prompt.
export const MentionCollection: Story = {
    tags: ["!dev"],
    render: () => (
        <PlannerAtStep
            seedStore={(seed) => {
                replay(REVIEW)(seed);
                useSetupWizardStore.getState().openPlannerPanel("Orders");
            }}
        />
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("button", { name: "Ask about Orders" }));

        const composer = canvas.getByRole("textbox", { name: "Prompt for the planner" });
        await waitFor(() => expect(composer).toHaveFocus());
        expect(composer).toHaveAttribute("placeholder", "Ask about Orders, or tell the planner what to change in it.");
        expect(canvas.getByRole("button", { name: "Remove Orders from the prompt" })).toBeInTheDocument();
    },
};

// Tables the planner left out go back to it as a prompt waiting in the composer.
export const AskToAddLeftOutTable: Story = {
    tags: ["!dev"],
    render: () => <PlannerAtStep seedStore={replay(REVIEW)} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("button", { name: "Show in Tables" }));
        await userEvent.click(canvas.getByRole("checkbox", { name: "Choose dbo.AuditLog to add to the plan" }));
        await userEvent.click(canvas.getByRole("button", { name: "Ask the planner to add it" }));

        expect(canvas.getByRole("textbox", { name: "Prompt for the planner" })).toHaveValue(
            "Add dbo.AuditLog to the plan.",
        );
    },
};

// Starting over clears the session and the planner starts again on the same tables.
export const StartOver: Story = {
    tags: ["!dev"],
    parameters: { msw: { handlers: startHandlers(migrationMocks.start([], { isEndless: true })) } },
    render: Review.render,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(canvasElement.ownerDocument.body);

        await userEvent.click(canvas.getByRole("button", { name: "Start over" }));
        const dialog = await body.findByRole("alertdialog", { name: "Start over?" });
        await userEvent.click(within(dialog).getByRole("button", { name: "Start over" }));

        await waitFor(() => expect(canvas.getByText("Planning collections")).toBeInTheDocument());
        expect(useSetupWizardStore.getState().plannerCollections).toEqual({});
    },
};
