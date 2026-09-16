import { queryOptions, type QueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import { toWizardStepError, WizardHandledError } from "@/components/form/wizard/wizard-step-error";
import type { WizardProgress } from "@/components/form/wizard/form-wizard";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableKey } from "@/pages/setup/add-app-wizard/discover-utils";

export const VERIFY_CDC_QUERY_KEY = ["setup", "verifyCdc"];

type SelectedTables = AppFormData["verifySchema"]["tables"];

export type VerifyCdcInput = {
    /** Everything the server keeps for this app lives under the connect key, the dry run included. */
    connectKey: string | null;
    slug: string;
    selectedTables: SelectedTables;
};

/**
 * The CDC dry run for one table selection. A query rather than a mutation because the outcome belongs
 * to the inputs it ran against: the cache entry is what lets "Verify schema" and Next share a single
 * spinner and a single error, and what keeps Next from repeating a run the operator already passed.
 */
export function verifyCdcQuery({ connectKey, slug, selectedTables }: VerifyCdcInput) {
    return queryOptions({
        // The connect key already embeds the source and the slug, so the selection is all that is left
        // to key on.
        queryKey: [...VERIFY_CDC_QUERY_KEY, connectKey, selectedTables.map(getTableKey).sort()],
        queryFn: () => verifyCdc(slug, selectedTables),
        // A dry run against an unchanged selection cannot go stale on its own, and a failed one is
        // retried by the operator rather than automatically.
        staleTime: Infinity,
        retry: false,
    });
}

/**
 * Runs the dry run a "Next" needs, or serves the pass already in the cache. Failures surface as
 * WizardHandledError: the step renders them from the cache entry, so the wizard must not alert again.
 */
export async function fetchVerifyCdc(
    queryClient: QueryClient,
    input: VerifyCdcInput,
    progress: WizardProgress,
    progressLabel: string,
): Promise<void> {
    const query = verifyCdcQuery(input);

    // A cached pass answers for these inputs on its own, so only a run that really happens is reported.
    if (queryClient.getQueryState(query.queryKey)?.status !== "success") {
        progress.report(progressLabel);
    }

    try {
        await queryClient.fetchQuery(query);
    } catch (error) {
        throw new WizardHandledError(error);
    }
}

async function verifyCdc(slug: string, selectedTables: SelectedTables) {
    const result = await api.services.setup.verifyCdc({
        tables: selectedTables.map((table) => ({
            sourceTableSchema: table.sourceTableSchema,
            sourceTableName: table.sourceTableName,
        })),
        slug,
    });

    if (!result.success) {
        const warningErrors = result.warnings.map((warning) => ({ message: warning }));
        throw toWizardStepError(
            [...result.errors, ...warningErrors],
            "Data source verification failed for the selected tables.",
        );
    }

    // Toasting from the fetcher stays a one-off: the entry never goes stale and is never retried.
    if (result.warnings.length > 0) {
        toast.warning("Data source verification passed with warnings", {
            description: result.warnings.join("\n"),
        });
    }

    return result;
}
