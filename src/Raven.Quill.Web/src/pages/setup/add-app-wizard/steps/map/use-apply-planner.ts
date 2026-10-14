import { useFormContext } from "react-hook-form";
import { api } from "@/api/api";
import type { MigrationApplyRequest, MigrationApplyResult } from "@/api/custom-services/migration-service";
import type { ApiErrorResponse } from "@/api/generated/server-api";
import { isApiError } from "@/api/http-client";
import { WizardStepError } from "@/components/form/wizard/wizard-step-error";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { wrapDtoTablesToFormShape } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import { parsePlannerTables, useAdoptMapTables } from "@/pages/setup/add-app-wizard/steps/map/use-adopt-map-tables";
import { selectBuiltCollections, selectIsPlanSettled } from "@/pages/setup/add-app-wizard/steps/map/planner-state";
import type { RegisteredCollection } from "@/pages/setup/add-app-wizard/steps/map/planner-types";

/**
 * Runs as the step's `beforeNext`: the plan is assembled server-side, validated as a whole, and
 * handed to the editor. Throwing here keeps the wizard on this step, which is the point - a plan
 * that does not assemble must not quietly advance. With nothing built, Next carries on with the
 * mapping the editor already holds.
 */
export function useApplyPlanner() {
    const { getValues } = useFormContext<AppFormData>();
    const adoptTables = useAdoptMapTables();

    return async () => {
        const store = useSetupWizardStore.getState();
        const built = selectBuiltCollections(store);
        const conversationId = store.plannerConversationId;
        const hasMapping = getValues("mapTables.tables").length > 0;

        if (!selectIsPlanSettled(store) || conversationId === null || built.length === 0) {
            if (hasMapping) {
                return;
            }

            throw new WizardStepError("The planner has not built a collection to apply yet.");
        }

        // Coming back to look at the plan and moving on again must not throw away what the
        // operator has since edited in the editor, so an unchanged plan is not applied twice.
        const originKey = computePlannerOriginKey(conversationId, built);
        if (hasMapping && originKey === store.mapOrigin?.key) {
            return;
        }

        const configuration = await applyPlan({
            slug: getValues("externalConnection").slug,
            conversationId,
            collections: built.map((collection) => collection.name),
        });

        // Tables the plan does not cover are not reported here: the editor's own UnmappedTablesAlert
        // derives the same list from the mapping it is about to render, and keeps deriving it as the
        // operator edits.
        adoptTables(parsePlannerTables(wrapDtoTablesToFormShape(configuration.tables ?? [])), {
            source: "planner",
            key: originKey,
        });
    };
}

/** Identifies what an apply would hand over: the session, and each built collection's registration. */
export function computePlannerOriginKey(conversationId: string, built: RegisteredCollection[]): string {
    const collections = built
        .map((collection) => `${collection.name}@${collection.registrationNumber}`)
        .sort()
        .join(",");

    return `planner:${conversationId}|${collections}`;
}

/** The server answers a plan that does not validate as a whole with 422 and the reasons. */
async function applyPlan(request: MigrationApplyRequest): Promise<NonNullable<MigrationApplyResult["configuration"]>> {
    try {
        const { configuration } = await api.services.migration.apply(request);

        if (!configuration) {
            throw new WizardStepError("The planner returned no configuration to apply.");
        }

        return configuration;
    } catch (error) {
        if (isApiError<ApiErrorResponse>(error) && error.status === 422) {
            throw new WizardStepError(
                "The registered mapping did not pass validation.",
                error.details?.errors?.join("\n") ?? error.message,
            );
        }

        throw error;
    }
}
