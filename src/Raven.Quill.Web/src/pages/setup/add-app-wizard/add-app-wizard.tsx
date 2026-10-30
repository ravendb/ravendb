import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router";
import { api } from "@/api/api";
import { isApiError } from "@/api/http-client";
import { ApiState } from "@/components/data/api-state";
import { FormFieldsSkeleton } from "@/components/data/loading-skeletons";
import { AppWizard } from "@/pages/setup/add-app-wizard/app-wizard";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { buildDraftSeed, hasCurrentPlan } from "@/pages/setup/add-app-wizard/draft-values";
import { DRAFT_SLUG_PARAM } from "@/pages/setup/add-app-wizard/use-save-draft";

export function AddAppWizard() {
    const [searchParams] = useSearchParams();
    // Read once: the wizard names its own slug there as it moves on, which must not load it again.
    const [draftSlug] = useState(() => searchParams.get(DRAFT_SLUG_PARAM));

    if (draftSlug) {
        return <ResumedAddAppWizard slug={draftSlug} />;
    }

    return <AppWizard defaultValues={getDefaultValues()} />;
}

function ResumedAddAppWizard({ slug }: { slug: string }) {
    // Not kept once the wizard is gone: a later visit would resume from what the planner has since moved past.
    const draftQuery = useQuery({ ...api.queries.setup.draft(slug), gcTime: 0 });
    const draft = draftQuery.data;
    const shouldReadPlan = !!draft && !draft.isAppCreated && hasCurrentPlan(draft);
    const planQuery = useQuery({ ...api.queries.setup.migrationPlan(slug), gcTime: 0, enabled: shouldReadPlan });
    const hasNoDraft = isApiError(draftQuery.error) && draftQuery.error.status === 404;

    // A created app is edited from its own configuration, so its leftover draft starts a new app instead.
    if (hasNoDraft || draft?.isAppCreated) {
        return <AppWizard defaultValues={getDefaultValues()} />;
    }

    // Mounted only once every response is in, so the wizard seeds itself exactly once.
    if (draft && (!shouldReadPlan || planQuery.data)) {
        const seed = buildDraftSeed(slug, draft, planQuery.data ?? null);

        return <AppWizard defaultValues={seed.values} resumedDraft={seed.resumedDraft} />;
    }

    return (
        <div className="p-8">
            <ApiState
                isLoading={draftQuery.isPending || (shouldReadPlan && planQuery.isPending)}
                isError={draftQuery.isError || planQuery.isError}
                errorTitle="Could not load the app you were adding"
                loadingLabel="Loading the app you were adding..."
                skeleton={<FormFieldsSkeleton count={4} />}
                onRetry={() => void (draftQuery.isError ? draftQuery.refetch() : planQuery.refetch())}
            >
                {null}
            </ApiState>
        </div>
    );
}

function getDefaultValues(): AppFormData {
    return {
        dataSource: {
            source: "external",
        },
        externalConnection: {
            appName: "",
            slug: "",
            provider: "",
            mode: "fields",
            fields: {
                host: "",
                port: null,
                database: "",
                username: "",
                password: "",
                ssl: "default",
            },
            connectionString: "",
        },
        verifySchema: {
            tables: [],
        },
        mapTables: {
            tables: [],
        },
        preview: {
            table: "",
            maxRows: 1,
        },
    };
}
