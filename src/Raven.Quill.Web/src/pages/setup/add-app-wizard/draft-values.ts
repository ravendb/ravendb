import type { MigrationPlanGetResponse, WizardDraftResponse } from "@/api/generated/server-api";
import type { ResumedDraft } from "@/pages/setup/add-app-wizard/app-wizard";
import { type AppFormData, type AppStepId, providerSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { buildConnectionSeed, DEFAULT_PROVIDER } from "@/pages/setup/add-app-wizard/connection-string";
import { resolveDefaultSchema } from "@/pages/setup/add-app-wizard/discover-utils";
import { toSlug } from "@/pages/setup/add-app-wizard/slugify";
import { computeConnectKey } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { haveSameTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";

export type DraftSeed = {
    values: AppFormData;
    resumedDraft: ResumedDraft;
};

/**
 * Only a plan made from the current selection counts: a new selection starts the planner over, but the
 * server keeps the old plan until a new proposal replaces it.
 */
export function hasCurrentPlan(draft: WizardDraftResponse): boolean {
    return (
        draft.hasPlan &&
        draft.discovery?.success === true &&
        haveSameTables(draft.selectedTables, draft.planSelectedTables)
    );
}

/**
 * Seeds a reloaded wizard from the draft the server stored for its slug. Work that never reached the
 * server, such as a mapping still in the editor or unsaved plan edits, starts over.
 */
export function buildDraftSeed(
    slug: string,
    draft: WizardDraftResponse,
    plan: MigrationPlanGetResponse | null,
): DraftSeed {
    const provider = providerSchema.catch(DEFAULT_PROVIDER).parse(draft.provider);
    const externalConnection: AppFormData["externalConnection"] = {
        appName: draft.appName,
        slug,
        provider,
        ...buildConnectionSeed(provider, draft.connectionString),
    };
    const discoverResult = draft.discovery?.success ? draft.discovery : null;
    const selectedTables = discoverResult ? draft.selectedTables : [];
    const connectKey = computeConnectKey(externalConnection);

    return {
        values: {
            dataSource: { source: "external" },
            externalConnection,
            verifySchema: { tables: selectedTables },
            mapTables: { tables: [] },
            preview: { table: "", maxRows: 1 },
        },
        resumedDraft: {
            initialStep: getResumeStep(draft),
            storeState: {
                discoverResult,
                discoverSchemas: draft.discoverSchemas,
                hasEditedSlug: toSlug(draft.appName) !== slug,
                connectKey: discoverResult ? connectKey : null,
                connectionAttempt: draft.isConnected ? { key: connectKey, error: null } : null,
                planner:
                    plan && hasCurrentPlan(draft)
                        ? {
                              session: {
                                  slug,
                                  connectKey,
                                  selectedTables,
                                  defaultSchema: resolveDefaultSchema(provider, draft.connectionString),
                              },
                              plan,
                          }
                        : null,
            },
        },
    };
}

function getResumeStep(draft: WizardDraftResponse): AppStepId {
    if (!draft.discovery?.success) {
        return "externalConnection";
    }

    return draft.step === "map" ? "map" : "verifySchema";
}
