import { useSetupWizardStore, type SetupWizardState } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { indexDiscovery } from "@/pages/setup/add-app-wizard/steps/map/session/planner-document";
import { buildPlan, type Plan, type PlanSource } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";

const PLAN_SOURCE_KEYS = [
    "plannerProposal",
    "plannerEntries",
    "plannerRetrying",
    "plannerRejectionIssues",
    "plannerStartedAreas",
    "plannerProposalChat",
    "plannerAreaChats",
    "plannerEdits",
    "plannerSelectedTables",
    "plannerDefaultSchema",
] as const satisfies readonly (keyof PlanSource)[];

/** The plan last built, kept so every view that reads it shares one build per change. */
let latestPlan: { state: SetupWizardState; plan: Plan } | null = null;

function selectPlan(state: SetupWizardState): Plan {
    const previous = latestPlan;
    const isUnchanged =
        previous !== null &&
        state.discoverResult === previous.state.discoverResult &&
        PLAN_SOURCE_KEYS.every((key) => state[key] === previous.state[key]);

    if (isUnchanged) {
        return previous.plan;
    }

    const plan = buildPlan(state, indexDiscovery(state.discoverResult));
    latestPlan = { state, plan };

    return plan;
}

export function usePlan(): Plan {
    return useSetupWizardStore(selectPlan);
}
