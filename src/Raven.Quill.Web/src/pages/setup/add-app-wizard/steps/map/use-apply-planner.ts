import { useFormContext } from "react-hook-form";
import { api } from "@/api/api";
import type { ApiErrorResponse, CdcSinkConfiguration } from "@/api/generated/server-api";
import { isApiError } from "@/api/http-client";
import type { WizardProgress } from "@/components/form/wizard/form-wizard";
import { WizardStepError } from "@/components/form/wizard/wizard-step-error";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { mapDtoTablesToForm } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import { formatTableScripts } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { isEditorMapping, type MapOrigin } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";
import type { PlannerState } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { parsePlannerTables, useAdoptMapTables } from "@/pages/setup/add-app-wizard/steps/map/use-adopt-map-tables";

/**
 * Runs before the app is created from the plan: the server turns the plan's registered collections into
 * the app's mapping, validated as a whole. Throwing keeps the wizard on this step - a plan that does not
 * assemble must not quietly become an app. A mapping made in the editor goes on to it untouched.
 */
export function useApplyPlanner() {
    const { getValues } = useFormContext<AppFormData>();
    const adoptTables = useAdoptMapTables();

    return async (progress: WizardProgress) => {
        const store = useSetupWizardStore.getState();

        if (isEditorMapping(store.mapOrigin)) {
            return;
        }

        if (!Object.values(store.plannerEntries).some((entry) => entry.status === "Registered")) {
            throw new WizardStepError("The planner has not built a collection yet.");
        }

        const slug = getValues("externalConnection").slug;
        const planKey = computePlanKey(slug, store.plannerEntries);

        if (getValues("mapTables.tables").length > 0 && isPlanApplied(store.mapOrigin, planKey)) {
            return;
        }

        progress.report("Applying the plan...");
        const configuration = await applyPlan(slug);

        adoptTables(parsePlannerTables(mapDtoTablesToForm(configuration.tables ?? []).map(formatTableScripts)), {
            source: "planner",
            planKey,
        });
    };
}

/** Identifies what an apply hands over: the app, and the version of each registered collection. */
export function computePlanKey(slug: string, entries: PlannerState["plannerEntries"]): string {
    const registered = Object.values(entries)
        .filter((entry) => entry.status === "Registered")
        .map((entry) => `${entry.name}@${entry.version}`)
        .sort()
        .join(",");

    return `${slug}|${registered}`;
}

export function isPlanApplied(origin: MapOrigin | null, planKey: string): boolean {
    return origin?.source === "planner" && origin.planKey === planKey;
}

/** The server answers a plan that does not validate as a whole with 422 and the reasons. */
async function applyPlan(slug: string): Promise<CdcSinkConfiguration> {
    try {
        const { configuration } = await api.services.setup.migrationApply({ slug });

        if (!configuration) {
            throw new WizardStepError("The planner returned no configuration to apply.");
        }

        return configuration;
    } catch (error) {
        if (isApiError<ApiErrorResponse>(error) && error.status === 422) {
            throw new WizardStepError(
                "The mapping did not pass validation.",
                error.details?.errors?.join("\n") ?? error.message,
            );
        }

        throw error;
    }
}
