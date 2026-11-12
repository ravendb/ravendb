import { useFormContext } from "react-hook-form";
import { useSearchParams } from "react-router";
import { api } from "@/api/api";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData, AppStepId } from "@/pages/setup/add-app-wizard/app-wizard-validation";

/** Names the new app's slug in the URL, so a reloaded wizard knows which draft to continue. */
export const DRAFT_SLUG_PARAM = "draft";

/**
 * Stores what only the form knows beside what the wizard's calls already stored for the slug, so a reloaded
 * wizard continues on the step it moved on to. An edited app reloads from its own configuration instead.
 */
export function useSaveDraft() {
    const { getValues } = useFormContext<AppFormData>();
    const [, setSearchParams] = useSearchParams();

    return async (nextStep: AppStepId) => {
        if (useSetupWizardStore.getState().editedAppSlug !== null) {
            return;
        }

        const { appName, slug } = getValues("externalConnection");

        // A failed save only costs a reload its restore, so it does not hold the wizard back.
        try {
            await api.services.setup.draftSave({
                slug,
                appName,
                selectedTables: getValues("verifySchema.tables"),
                step: nextStep,
            });
            setSearchParams({ [DRAFT_SLUG_PARAM]: slug }, { replace: true });
        } catch (error) {
            console.error("Could not save the wizard draft:", error);
        }
    };
}
