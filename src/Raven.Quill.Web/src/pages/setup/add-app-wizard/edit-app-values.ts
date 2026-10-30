import type {
    AppCdcConfigurationResponse,
    ApplianceAppResponse,
    MigrationPlanGetResponse,
    WizardDraftResponse,
} from "@/api/generated/server-api";
import { type AppFormData, tablesSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { collectConfigSchemas } from "@/pages/setup/add-app-wizard/config-io";
import { buildConnectionSeed } from "@/pages/setup/add-app-wizard/connection-string";
import { resolveProviderFromSourceType } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-options";
import { computeConnectKey } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { mapDtoTablesToForm } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import { getTableLabel, resolveDefaultSchema } from "@/pages/setup/add-app-wizard/discover-utils";
import type { EditedApp } from "@/pages/setup/add-app-wizard/app-wizard";

export type EditAppSeed = {
    values: AppFormData;
    editedApp: EditedApp;
};

/**
 * Seeds the wizard from an app's stored CDC Sink configuration. An app whose mapping the planner applied
 * reopens that plan, as long as the plan still holds what was applied; any other goes to the editor, so
 * nothing asks the AI to redo work the app already runs on.
 *
 * Returns a reason instead of a seed when the stored mapping does not fit the editor's shape, which
 * means it was written outside the wizard - every path here validates against `tablesSchema`.
 */
export function buildEditAppSeed(
    app: ApplianceAppResponse,
    cdc: AppCdcConfigurationResponse,
    draft: WizardDraftResponse | null,
    plan: MigrationPlanGetResponse | null,
): EditAppSeed | { error: string } {
    const configTables = cdc.configuration.tables ?? [];
    const parsedTables = tablesSchema.safeParse(mapDtoTablesToForm(configTables));

    if (!parsedTables.success) {
        return {
            error:
                parsedTables.error.issues[0]?.message ??
                "The stored table mapping does not match the shape the editor understands.",
        };
    }

    const tables = parsedTables.data;
    const provider = resolveProviderFromSourceType(app.source.type);
    const connectionString = cdc.connectionString ?? "";
    const externalConnection: AppFormData["externalConnection"] = {
        appName: app.name,
        slug: app.slug,
        provider,
        ...buildConnectionSeed(provider, connectionString),
    };
    const appliedPlan = draft && plan && isPlanAsApplied(draft, plan) ? { draft, plan } : null;
    // The planner was handed the selection, which also covers tables the mapping embeds rather than roots.
    const selectedTables = (appliedPlan ? appliedPlan.draft.planSelectedTables : tables).map((table) => ({
        sourceTableSchema: table.sourceTableSchema ?? null,
        sourceTableName: table.sourceTableName,
    }));

    return {
        values: {
            dataSource: { source: "external" },
            externalConnection,
            verifySchema: { tables: selectedTables },
            mapTables: { tables },
            preview: { table: getTableLabel(tables[0]), maxRows: 1 },
        },
        editedApp: {
            slug: app.slug,
            discoverSchemas: appliedPlan ? appliedPlan.draft.discoverSchemas : collectConfigSchemas(configTables),
            initialSelectedTables: selectedTables,
            planner: appliedPlan
                ? {
                      session: {
                          slug: app.slug,
                          connectKey: computeConnectKey(externalConnection),
                          selectedTables,
                          defaultSchema: resolveDefaultSchema(provider, connectionString),
                      },
                      plan: appliedPlan.plan,
                  }
                : null,
        },
    };
}

/** Whether the plan still holds the very collection versions the app's mapping was applied from. */
export function isPlanAsApplied(draft: WizardDraftResponse, plan: MigrationPlanGetResponse): boolean {
    const appliedVersions = draft.appliedPlanVersions;

    if (!appliedVersions || draft.planSelectedTables.length === 0) {
        return false;
    }

    const registered = Object.entries(plan.plan.entries ?? {}).filter(
        ([, entry]) => (entry.status ?? "Registered") === "Registered",
    );

    return (
        registered.length === Object.keys(appliedVersions).length &&
        registered.every(([name, entry]) => appliedVersions[name] === (entry.version ?? 1))
    );
}
