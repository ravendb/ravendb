import { useMutation } from "@tanstack/react-query";
import { useState } from "react";
import { useFormContext } from "react-hook-form";
import { toast } from "sonner";
import type { DiscoverResponse } from "@/api/generated/server-api";
import { api } from "@/api/api";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData, tablesSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    collectConfigSchemas,
    collectSourceTableRefs,
    parseConfigFile,
    type WizardConfig,
} from "@/pages/setup/add-app-wizard/config-io";
import { parseConnectionString } from "@/pages/setup/add-app-wizard/connection-string";
import {
    getTableLabel,
    isTableSupported,
    MAX_SELECTED_TABLES,
    resolveDefaultSchema,
} from "@/pages/setup/add-app-wizard/discover-utils";
import { wrapDtoTablesToFormShape } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import { findDiscoveredTable } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { createDraftSlug, toSlug } from "@/pages/setup/add-app-wizard/slugify";
import { discoverTables } from "@/pages/setup/add-app-wizard/steps/verify/use-discover-tables";
import { computeConnectKey } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { toWizardStepError } from "@/components/form/wizard/wizard-step-error";
import { useAdoptMapTables } from "@/pages/setup/add-app-wizard/steps/map/use-adopt-map-tables";

const IMPORT_PHASES = {
    reading: "Reading the configuration...",
    connecting: "Testing the connection...",
    discovering: "Discovering tables...",
    verifying: "Verifying mapped tables...",
} as const;

type ImportResult = {
    config: WizardConfig;
    slug: string;
    isDraftSlug: boolean;
    formTables: AppFormData["mapTables"]["tables"];
    discoverResult: DiscoverResponse;
    schemas: string[];
};

/**
 * The export carries no slug, and the import can run before the app is even named, so a draft slug
 * stands in for the server calls. The form keeps its slug empty in that case: naming the app later
 * changes the connect key, which is what makes the connect step re-run under the real slug.
 */
function resolveImportSlug(connection: AppFormData["externalConnection"]): { slug: string; isDraft: boolean } {
    const slug = connection.slug.trim() || toSlug(connection.appName);

    return slug ? { slug, isDraft: false } : { slug: createDraftSlug(), isDraft: true };
}

export function useImportConfig() {
    const { setValue, getValues } = useFormContext<AppFormData>();
    const adoptTables = useAdoptMapTables();
    const setDiscoverResult = useSetupWizardStore((state) => state.setDiscoverResult);
    const setConnectionAttempt = useSetupWizardStore((state) => state.setConnectionAttempt);
    const setConnectKey = useSetupWizardStore((state) => state.setConnectKey);
    const setInitialSelectedTables = useSetupWizardStore((state) => state.setInitialSelectedTables);
    const [progressLabel, setProgressLabel] = useState<string>(IMPORT_PHASES.reading);

    const importMutation = useMutation<ImportResult, Error, File>({
        mutationFn: async (file) => {
            setProgressLabel(IMPORT_PHASES.reading);
            const config = await parseConfigFile(file);

            if (config.tables.length > MAX_SELECTED_TABLES) {
                throw new Error(
                    `The configuration maps ${config.tables.length} tables, but one app processes at most ${MAX_SELECTED_TABLES}. Support for unlimited tables is coming later.`,
                );
            }

            const connectionValues = getValues("externalConnection");
            const { slug, isDraft: isDraftSlug } = resolveImportSlug(connectionValues);
            const connection = {
                ...connectionValues,
                provider: config.provider,
                mode: "raw" as const,
                connectionString: config.connectionString,
                slug,
            };

            setProgressLabel(IMPORT_PHASES.connecting);
            const connectResult = await api.services.setup.connect({
                connectionString: config.connectionString,
                provider: config.provider,
                slug,
            });
            const connectError = connectResult.success
                ? null
                : toWizardStepError(connectResult.errors, "Connection failed.");

            // The server now holds the imported source, also when the import goes no further.
            setConnectionAttempt({ key: computeConnectKey(connection), error: connectError });

            if (connectError) {
                throw connectError;
            }

            // Discover every schema the configuration touches, not just the default one, so tables
            // in custom schemas can still be verified.
            setProgressLabel(IMPORT_PHASES.discovering);
            const schemas = collectConfigSchemas(config.tables);
            const discoverResult = await discoverTables(connection, schemas, slug);

            if (!discoverResult.success) {
                throw toWizardStepError(discoverResult.errors, "Could not discover tables.");
            }

            setProgressLabel(IMPORT_PHASES.verifying);
            const defaultSchema = resolveDefaultSchema(config.provider, config.connectionString);
            const unavailable = collectSourceTableRefs(config.tables).filter((ref) => {
                const discovered = findDiscoveredTable(
                    discoverResult,
                    ref.sourceTableSchema ?? defaultSchema,
                    ref.sourceTableName,
                );
                return !discovered || !isTableSupported(discoverResult, discovered);
            });

            if (unavailable.length > 0) {
                throw new Error(
                    `These tables from the configuration are not available or supported on the connected database: ${unavailable
                        .map((ref) => getTableLabel(ref))
                        .join(", ")}.`,
                );
            }

            try {
                const formTables = tablesSchema.parse(wrapDtoTablesToFormShape(config.tables));
                return { config, slug, isDraftSlug, formTables, discoverResult, schemas };
            } catch {
                throw new Error(
                    "The configuration's table mapping is invalid or was exported from an incompatible version.",
                );
            }
        },
        onSuccess: ({ config, slug, isDraftSlug, formTables, discoverResult, schemas }) => {
            toast.success("Configuration imported");

            const verifySchemaTables = config.tables.map((table) => ({
                sourceTableSchema: table.sourceTableSchema ?? null,
                sourceTableName: table.sourceTableName ?? "",
            }));

            const { values: fields, droppedKeywords } = parseConnectionString(config.connectionString);

            setValue("externalConnection.provider", config.provider);
            setValue("externalConnection.mode", droppedKeywords.length === 0 ? "fields" : "raw");
            setValue("externalConnection.fields", fields);
            setValue("externalConnection.connectionString", config.connectionString);
            // A real slug is the one later steps key their work by, so the form must carry it. A draft
            // one is never shown: the operator still has to name the app, and the slug follows from that.
            if (!isDraftSlug) {
                setValue("externalConnection.slug", slug, { shouldValidate: true });
            }
            setValue("verifySchema.tables", verifySchemaTables);
            adoptTables(formTables, { source: "import" });
            setValue("preview.table", getTableLabel(formTables[0]));
            setDiscoverResult(discoverResult, schemas);

            const connection = getValues("externalConnection");
            const connectKey = computeConnectKey(connection);

            setConnectKey(connectKey);
            setConnectionAttempt({ key: connectKey, error: null });
            setInitialSelectedTables(verifySchemaTables);
        },
    });

    return { importMutation, progressLabel };
}
