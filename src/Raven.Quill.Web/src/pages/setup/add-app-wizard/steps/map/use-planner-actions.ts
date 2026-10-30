import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { getRootTablePath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { usePlannerWorkspace } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { PlanArea, PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { composeFixCollectionPrompt } from "@/pages/setup/add-app-wizard/steps/map/session/planner-prompts";
import {
    getChatPhase,
    PLAN_VIEW,
    selectHasUnsavedEdits,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

export function usePlannerActions() {
    const session = usePlannerSession();
    const showPlannerView = useSetupWizardStore((state) => state.showPlannerView);
    const setPlannerChatOpen = useSetupWizardStore((state) => state.setPlannerChatOpen);
    const setMapActiveTable = useSetupWizardStore((state) => state.setMapActiveTable);
    const resetMapTablesUiState = useSetupWizardStore((state) => state.resetMapTablesUiState);

    const openArea = (area: string) => showPlannerView({ area, collection: null });

    // The page edits the mapping with the Map schema editor, which holds the collection as its one table.
    const openCollection = (area: string, collection: string) => {
        resetMapTablesUiState();
        setMapActiveTable({ type: "root", path: getRootTablePath(0) });
        showPlannerView({ area, collection });
    };

    const buildArea = (area: PlanArea) => {
        setPlannerChatOpen(true);
        void session.buildArea(area.name, area.pendingCollections);
    };

    return {
        session,
        openPlan: () => showPlannerView(PLAN_VIEW),
        openArea,
        openCollection,
        /** A collection named in a chat opens on its page once mapped, or in its area until then. */
        openCollectionByName: (collection: string) => {
            const state = useSetupWizardStore.getState();
            const entry = state.plannerEntries[collection];
            const area =
                entry?.area ??
                state.plannerProposal?.areas.find((proposed) =>
                    proposed.collections.some((proposedCollection) => proposedCollection.collection === collection),
                )?.area;

            if (!area) {
                return;
            }

            if (entry?.config && entry.status !== "Removed") {
                openCollection(area, collection);
            } else {
                openArea(area);
            }
        },
        buildArea,
        buildAndOpenArea: (area: PlanArea) => {
            buildArea(area);
            openArea(area.name);
        },
        askToFix: (collection: PlanCollection) => {
            setPlannerChatOpen(true);
            void session.askArea(collection.area, {
                text: composeFixCollectionPrompt(collection.name, collection.problems),
                mentions: [],
            });
        },
    };
}

/**
 * Why Build waits, or an empty string when it does not. The proposal agent's questions settle the
 * conventions, and an area builds with the conventions in force when it starts.
 */
export function useBuildBlockReason(): string {
    const { isBusy } = usePlannerWorkspace();
    const proposalPhase = useSetupWizardStore((state) => getChatPhase(state.plannerProposalChat));
    const isSaving = useSetupWizardStore((state) => state.plannerIsSaving);
    const hasUnsavedEdits = useSetupWizardStore(selectHasUnsavedEdits);

    if (proposalPhase === "awaitingAnswers") {
        return "Answer the planner's questions first.";
    }

    if (proposalPhase === "working") {
        return "Wait for the planner to finish the plan.";
    }

    if (isSaving) {
        return "Wait for the plan to save.";
    }

    if (hasUnsavedEdits) {
        return "Save or discard your changes first.";
    }

    if (isBusy) {
        return "Wait for the wizard to finish.";
    }

    return "";
}
