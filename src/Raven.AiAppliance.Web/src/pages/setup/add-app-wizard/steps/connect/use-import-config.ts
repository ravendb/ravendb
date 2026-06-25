import { useMutation } from "@tanstack/react-query";
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
import { isTableSupported } from "@/pages/setup/add-app-wizard/discover-utils";
import { wrapDtoTablesToFormShape } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import {
    findDiscoveredTable,
    getSourceTableLabel,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { discoverTables } from "@/pages/setup/add-app-wizard/steps/verify/use-discover-tables";
import { computeConnectKey } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { computeMapKey } from "@/pages/setup/add-app-wizard/steps/map/use-map-schema-step";

type ImportResult = {
    config: WizardConfig;
    formTables: AppFormData["mapTables"]["tables"];
    discoverResult: DiscoverResponse;
    schemas: string[];
};

export function useImportConfig() {
    const { setValue } = useFormContext<AppFormData>();

    return useMutation<ImportResult, Error, File>({
        mutationFn: async (file) => {
            const config = await parseConfigFile(file);

            const connectResult = await api.services.setup.connect({
                connectionString: config.connectionString,
                provider: config.provider,
            });

            if (!connectResult.success) {
                throw new Error(connectResult.errors?.join("\n") || "Connection failed.");
            }

            // Discover every schema the configuration touches, not just the default one, so tables
            // in custom schemas can still be verified.
            const schemas = collectConfigSchemas(config.tables);
            const discoverResult = await discoverTables(
                { appName: "", provider: config.provider, connectionString: config.connectionString },
                schemas,
            );

            if (!discoverResult.success) {
                throw new Error(discoverResult.errors?.join("\n") || "Could not discover tables.");
            }

            const unavailable = collectSourceTableRefs(config.tables).filter((ref) => {
                const discovered = findDiscoveredTable(discoverResult, ref.sourceTableSchema, ref.sourceTableName);
                return !discovered || !isTableSupported(discoverResult, discovered);
            });

            if (unavailable.length > 0) {
                throw new Error(
                    `These tables from the configuration are not available or supported on the connected database: ${unavailable
                        .map((ref) => getSourceTableLabel(ref))
                        .join(", ")}.`,
                );
            }

            try {
                const formTables = tablesSchema.parse(wrapDtoTablesToFormShape(config.tables));
                return { config, formTables, discoverResult, schemas };
            } catch {
                throw new Error(
                    "The configuration's table mapping is invalid or was exported from an incompatible version.",
                );
            }
        },
        onSuccess: ({ config, formTables, discoverResult, schemas }) => {
            toast.success("Configuration imported");

            const verifySchemaTables = config.tables.map((table) => ({
                sourceTableSchema: table.sourceTableSchema ?? null,
                sourceTableName: table.sourceTableName ?? "",
            }));

            setValue("externalConnection.provider", config.provider);
            setValue("externalConnection.connectionString", config.connectionString);
            setValue("verifySchema.tables", verifySchemaTables);
            setValue("map.source", "manual");
            setValue("map.aiPrompt", "");
            setValue("mapTables.tables", formTables);
            setValue("preview.table", getSourceTableLabel(formTables[0]) ?? "");

            const store = useSetupWizardStore.getState();
            store.setDiscoverResult(discoverResult, schemas);
            store.resetMapTablesUiState();

            store.setConnectKey(computeConnectKey(config));
            store.setAppliedMapKey(
                computeMapKey({ source: "manual", aiPrompt: "", selectedTables: verifySchemaTables }),
            );
            store.lockImportedConfig();
        },
    });
}
