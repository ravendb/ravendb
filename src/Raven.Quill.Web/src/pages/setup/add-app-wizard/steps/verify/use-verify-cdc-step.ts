import { useIsFetching, useQuery, useQueryClient } from "@tanstack/react-query";
import { useFormContext, useWatch } from "react-hook-form";
import { WizardHandledError } from "@/components/form/wizard/wizard-step-error";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    VERIFY_CDC_QUERY_KEY,
    verifyCdcQuery,
    type VerifyCdcInput,
} from "@/pages/setup/add-app-wizard/steps/verify/verify-cdc-query";
import type { WizardProgress } from "@/components/form/wizard/form-wizard";

/** The dry-run inputs the form and the store currently hold. */
function useVerifyCdcInput(): VerifyCdcInput {
    const { control } = useFormContext<AppFormData>();
    const connectKey = useSetupWizardStore((state) => state.connectKey);

    // Watched rather than read through getValues, otherwise React Compiler memoizes the input against
    // the stable getValues reference and it never follows the selection.
    const [slug, selectedTables] = useWatch({ control, name: ["externalConnection.slug", "verifySchema.tables"] });

    return { connectKey, slug, selectedTables };
}

/**
 * Whether a dry run is in flight, for any selection - including one still finishing for a selection the
 * operator has since changed. It freezes everything that would invalidate the run under way: the table
 * selection, the schema list it was discovered from, and Next. Deliberately reads nothing but the fetch
 * state, so the wizard shell can hold Next back without re-rendering on every checkbox.
 */
export function useIsVerifyCdcRunning(): boolean {
    return useIsFetching({ queryKey: VERIFY_CDC_QUERY_KEY }) > 0;
}

/**
 * The dry run for the current selection. The query is only observed here, never enabled: the call
 * provisions capture infrastructure on the source, so it runs when the operator asks for it or when
 * Next needs it, and both read their state from this one cache entry.
 */
export function useVerifyCdcState() {
    const queryClient = useQueryClient();
    const input = useVerifyCdcInput();
    const { isFetching, isSuccess, error } = useQuery({ ...verifyCdcQuery(input), enabled: false });
    const isRunning = useIsVerifyCdcRunning();

    return {
        isVerifying: isFetching,
        isVerified: isSuccess,
        isRunning,
        error,
        // The rejection is the query's own error, which the step already renders from the cache.
        verify: () => void queryClient.fetchQuery(verifyCdcQuery(input)).catch(() => {}),
    };
}

export function useVerifyCdcStep() {
    const queryClient = useQueryClient();
    const { getValues } = useFormContext<AppFormData>();

    return async (progress: WizardProgress) => {
        const { connectKey } = useSetupWizardStore.getState();
        const { slug } = getValues("externalConnection");

        try {
            progress.report("Verifying schema...");
            // Serves the cached pass when this selection was already verified, here or from the button.
            await queryClient.fetchQuery(
                verifyCdcQuery({ connectKey, slug, selectedTables: getValues("verifySchema.tables") }),
            );
        } catch (error) {
            // The step renders the failure from the query, so the wizard must not alert about it again.
            throw new WizardHandledError(error);
        }
    };
}
