import type { AppCdcConfigurationResponse, ApplianceAppResponse } from "@/api/generated/server-api";
import { type AppFormData, tablesSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { collectConfigSchemas } from "@/pages/setup/add-app-wizard/config-io";
import {
    DEFAULT_PORT_BY_PROVIDER,
    type Provider,
    parseConnectionString,
} from "@/pages/setup/add-app-wizard/connection-string";
import { resolveProviderFromSourceType } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-options";
import { wrapDtoTablesToFormShape } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import { getSourceTableLabel } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import type { EditedApp } from "@/pages/setup/add-app-wizard/app-wizard";

export type EditAppSeed = {
    values: AppFormData;
    editedApp: EditedApp;
};

/**
 * Seeds the wizard from an app's stored CDC Sink configuration, pinning the mapping source to
 * "manual" so nothing asks the AI to redo work the app already runs on.
 *
 * Returns a reason instead of a seed when the stored mapping does not fit the editor's shape, which
 * means it was written outside the wizard - every path here validates against `tablesSchema`.
 */
export function buildEditAppSeed(
    app: ApplianceAppResponse,
    cdc: AppCdcConfigurationResponse,
): EditAppSeed | { error: string } {
    const configTables = cdc.configuration.tables ?? [];
    const parsedTables = tablesSchema.safeParse(wrapDtoTablesToFormShape(configTables));

    if (!parsedTables.success) {
        return {
            error:
                parsedTables.error.issues[0]?.message ??
                "The stored table mapping does not match the shape the editor understands.",
        };
    }

    const tables = parsedTables.data;
    const provider = resolveProviderFromSourceType(app.source.type);
    const selectedTables = tables.map((table) => ({
        sourceTableSchema: table.sourceTableSchema,
        sourceTableName: table.sourceTableName,
    }));

    return {
        values: {
            dataSource: { source: "external" },
            externalConnection: {
                appName: app.name,
                slug: app.slug,
                provider,
                ...buildConnectionSeed(provider, cdc.connectionString ?? ""),
            },
            verifySchema: { tables: selectedTables },
            map: { source: "manual", aiPrompt: "" },
            mapTables: { tables },
            preview: { table: getSourceTableLabel(tables[0]) ?? "", maxRows: 1 },
        },
        editedApp: {
            slug: app.slug,
            discoverSchemas: collectConfigSchemas(configTables),
            initialSelectedTables: selectedTables,
        },
    };
}

type ConnectionSeed = Pick<AppFormData["externalConnection"], "mode" | "fields" | "connectionString">;

/** Mirrors the config import: parsed fields when the stored string parses cleanly, the raw string
 * otherwise, so nothing the app connects with is dropped. */
function buildConnectionSeed(provider: Provider, connectionString: string): ConnectionSeed {
    if (connectionString === "") {
        return {
            mode: "fields",
            fields: {
                host: "",
                port: DEFAULT_PORT_BY_PROVIDER[provider],
                database: "",
                username: "",
                password: "",
                ssl: "default",
            },
            connectionString: "",
        };
    }

    const { values: fields, droppedKeywords } = parseConnectionString(connectionString);

    return { mode: droppedKeywords.length === 0 ? "fields" : "raw", fields, connectionString };
}
