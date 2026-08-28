import { z } from "zod";
import type {
    AppCdcConfigurationResponse,
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkTableConfig,
} from "@/api/generated/server-api";
import { type AppFormData, providerSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { resolveConnectionString } from "@/pages/setup/add-app-wizard/connection-string";
import { resolveProviderFromSourceType } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-options";
import { mapFormTablesToDto } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";

/** The portable wizard configuration: connection details plus the CDC Sink table mapping, in the
 * canonical DTO shape (matching `/setup/map`). The app name and slug are excluded - a slug
 * is unique per app, so an imported one could only conflict. */
export type WizardConfig = {
    provider: AppFormData["externalConnection"]["provider"];
    connectionString: string;
    tables: CdcSinkTableConfig[];
};

export type SourceTableRef = {
    sourceTableSchema: string | null;
    sourceTableName: string;
};

// Tables are validated structurally on top level only; the deep CDC graph is validated later by
// `tablesSchema` once converted to the form shape, which yields friendlier per-field messages.
const wizardConfigSchema = z.object({
    provider: providerSchema,
    connectionString: z.string().trim().min(1, "The configuration is missing a connection string."),
    tables: z.array(z.looseObject({})).min(1, "The configuration does not define any tables."),
});

export function buildConfigExport(values: AppFormData): WizardConfig {
    return {
        provider: values.externalConnection.provider,
        connectionString: resolveConnectionString(values.externalConnection),
        tables: mapFormTablesToDto(values.mapTables.tables),
    };
}

// Builds the same portable configuration from an existing app's stored CDC mapping, so the
// data-source view can export it without going through the wizard. The stored tables are already in
// the canonical DTO shape, and the provider is recovered from the app's reported source type.
export function buildConfigExportFromCdc(sourceType: string, cdc: AppCdcConfigurationResponse): WizardConfig {
    return {
        provider: resolveProviderFromSourceType(sourceType),
        connectionString: cdc.connectionString ?? "",
        tables: cdc.configuration.tables ?? [],
    };
}

export function downloadConfig(config: WizardConfig, fileName = "Quill-app-config.json") {
    const blob = new Blob([JSON.stringify(config, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");

    anchor.href = url;
    anchor.download = fileName;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
}

export async function parseConfigFile(file: File): Promise<WizardConfig> {
    let raw: string;

    try {
        raw = await file.text();
    } catch {
        throw new Error("Could not read the selected file.");
    }

    let json: unknown;

    try {
        json = JSON.parse(raw);
    } catch {
        throw new Error("The selected file is not valid JSON.");
    }

    const result = wizardConfigSchema.safeParse(json);

    if (!result.success) {
        throw new Error(result.error.issues[0]?.message || "The file does not look like an exported configuration.");
    }

    return {
        provider: result.data.provider,
        connectionString: result.data.connectionString,
        tables: result.data.tables as CdcSinkTableConfig[],
    };
}

type AnyTableConfig = {
    sourceTableSchema?: string | null;
    sourceTableName?: string | null;
    embeddedTables?: CdcSinkEmbeddedTableConfig[] | null;
    linkedTables?: CdcSinkLinkedTableConfig[] | null;
};

/** Every distinct source table referenced anywhere in the config (root, embedded, or linked). */
export function collectSourceTableRefs(tables: CdcSinkTableConfig[]): SourceTableRef[] {
    const refs = new Map<string, SourceTableRef>();

    const visit = (value: unknown) => {
        if (value === null || typeof value !== "object") {
            return;
        }

        const table = value as AnyTableConfig;
        const name = table.sourceTableName?.trim();

        if (name) {
            const schema = table.sourceTableSchema?.trim() || null;
            refs.set(`${schema ?? ""}::${name}`, { sourceTableSchema: schema, sourceTableName: name });
        }

        (Array.isArray(table.embeddedTables) ? table.embeddedTables : []).forEach(visit);
        (Array.isArray(table.linkedTables) ? table.linkedTables : []).forEach(visit);
    };

    tables.forEach(visit);

    return [...refs.values()];
}

/** Distinct non-default schemas referenced by the config, used to scope discovery on import. */
export function collectConfigSchemas(tables: CdcSinkTableConfig[]): string[] {
    return [
        ...new Set(
            collectSourceTableRefs(tables)
                .map((ref) => ref.sourceTableSchema)
                .filter((schema): schema is string => Boolean(schema)),
        ),
    ];
}
