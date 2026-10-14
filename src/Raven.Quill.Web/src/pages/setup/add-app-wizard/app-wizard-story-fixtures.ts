import type { MigrationFrame } from "@/api/custom-services/migration-service";
import type { AiHelperStatus, DiscoverResponse } from "@/api/generated/server-api";
import { assistantMocks } from "@/mocks/assistant-mocks";
import { defaultApiMocks } from "@/mocks/default-mocks";
import { sampleDiscovery, setupMocks } from "@/mocks/setup-mocks";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { isTableSupported } from "@/pages/setup/add-app-wizard/discover-utils";
import { scaffoldRootTable } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import type { PlannerAnswersTurn, PlannerPromptTurn } from "@/pages/setup/add-app-wizard/steps/map/planner-types";

// Valid values for every step, so any step renders with realistic data and Next/Back keep
// working against the default MSW mocks. The verify step's initial selection follows the
// discovery being shown; map/preview always seed from the clean happy-path discovery.
export function buildSeed(discovery: DiscoverResponse): AppFormData {
    return {
        dataSource: { source: "external" },
        externalConnection: {
            appName: "AcmeShop",
            slug: "acme-shop",
            provider: "Npgsql",
            mode: "fields",
            fields: {
                host: "localhost",
                port: 5432,
                database: "acme_shop",
                username: "admin",
                password: "secret",
                ssl: "disable",
            },
            connectionString: "",
        },
        verifySchema: {
            tables: discovery.tables
                .filter((table) => isTableSupported(discovery, table))
                .map((table) => ({
                    sourceTableSchema: table.sourceTableSchema,
                    sourceTableName: table.sourceTableName,
                })),
        },
        mapTables: { tables: sampleDiscovery.tables.map((table) => scaffoldRootTable(sampleDiscovery, table)) },
        preview: { table: "dbo.Customers", maxRows: 1 },
    };
}

// Replaces the discover handler so re-discovery ("Customize schemas") stays consistent
// with the seeded result, keeping the rest of the setup mocks intact.
export function discoverHandlers(discovery: DiscoverResponse) {
    return { setup: [setupMocks.discover(discovery), ...defaultApiMocks.setup] };
}

export function consentHandlers(status: AiHelperStatus) {
    return { assistant: [assistantMocks.consent({ status }), ...defaultApiMocks.assistant] };
}

export type PlannerScriptTurn = {
    /** What the operator said; omitted for the turn that opens the session. */
    userTurn?: PlannerPromptTurn | PlannerAnswersTurn;
    frames: MigrationFrame[];
    /** Leaves the turn running, as if its stream were still open. */
    isWorking?: boolean;
};

/** Plays recorded turns through the store, so a story starts from a session the planner really produced. */
export function replayPlannerTurns(turns: PlannerScriptTurn[], selectedTables: AppFormData["verifySchema"]["tables"]) {
    const store = useSetupWizardStore.getState();

    for (const turn of turns) {
        if (turn.userTurn) {
            store.beginPlannerTurn({ userTurn: turn.userTurn, controller: new AbortController() });
        } else {
            store.openPlannerSession({ selectedTables, controller: new AbortController() });
        }

        for (const frame of turn.frames) {
            store.receivePlannerFrame(frame);
        }

        if (!turn.isWorking) {
            store.endPlannerTurn({ error: null, isStopped: false });
        }
    }
}
