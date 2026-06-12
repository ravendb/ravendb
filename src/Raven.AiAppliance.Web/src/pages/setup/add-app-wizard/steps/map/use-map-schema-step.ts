import { api } from "@/api/api";
import type {
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkTableConfig,
    DiscoverResponse,
} from "@/api/generated/server-api";
import { toStringValueItems } from "@/lib/form-utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData, tablesSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    createEmptyRootTable,
    findDiscoveredTable,
    pascalCase,
    scaffoldRootTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { useFormContext } from "react-hook-form";

export function useMapSchemaStep() {
    const { getValues, setValue } = useFormContext<AppFormData>();

    return async () => {
        const { source, aiPrompt } = getValues("map");
        const selectedTables = getValues("verifySchema.tables");
        const store = useSetupWizardStore.getState();

        const appliedMapKey = JSON.stringify({
            source,
            aiPrompt: source === "ai-suggested" ? aiPrompt.trim() : "",
            selectedTables,
        });

        // Same inputs as the last generation - keep the (possibly edited) tables.
        if (appliedMapKey === store.appliedMapKey && getValues("mapTables.tables").length > 0) {
            return;
        }

        const tables =
            source === "ai-suggested"
                ? await suggestTables(aiPrompt)
                : scaffoldTables(selectedTables, store.discoverResult);

        setValue("mapTables.tables", tables);
        store.setAppliedMapKey(appliedMapKey);
        store.resetMapTablesUiState();
    };
}

async function suggestTables(aiPrompt: string): Promise<AppFormData["mapTables"]["tables"]> {
    const result = await api.services.setup.suggestCdc({
        intentPrompt: aiPrompt.trim(),
    });

    if (result.status !== "Success" || !result.configuration) {
        throw new Error(result.rationale.filter(Boolean).join("\n") || `AI suggestion failed (${result.status}).`);
    }

    return tablesSchema.parse((result.configuration.tables ?? []).map(wrapTableStringLists));
}

// The DTO stores string lists as plain string[], while the form stores them as
// { value } items (the FormStringList/useFieldArray shape). Wrap them before the
// zod parse that validates the suggested configuration.
function wrapTableStringLists(table: CdcSinkTableConfig) {
    return {
        ...table,
        primaryKeyColumns: toStringValueItems(table.primaryKeyColumns),
        embeddedTables: (table.embeddedTables ?? []).map(wrapEmbeddedTableStringLists),
        linkedTables: (table.linkedTables ?? []).map(wrapLinkedTableStringLists),
    };
}

function wrapEmbeddedTableStringLists(table: CdcSinkEmbeddedTableConfig): unknown {
    return {
        ...table,
        primaryKeyColumns: toStringValueItems(table.primaryKeyColumns),
        joinColumns: toStringValueItems(table.joinColumns),
        embeddedTables: (table.embeddedTables ?? []).map(wrapEmbeddedTableStringLists),
        linkedTables: (table.linkedTables ?? []).map(wrapLinkedTableStringLists),
    };
}

function wrapLinkedTableStringLists(table: CdcSinkLinkedTableConfig) {
    return {
        ...table,
        joinColumns: toStringValueItems(table.joinColumns),
    };
}

function scaffoldTables(
    selectedTables: AppFormData["verifySchema"]["tables"],
    discoverResult: DiscoverResponse | null,
): AppFormData["mapTables"]["tables"] {
    return selectedTables.map((selected) => {
        const discovered = findDiscoveredTable(discoverResult, selected.sourceTableSchema, selected.sourceTableName);

        if (discovered) {
            return scaffoldRootTable(discoverResult, discovered);
        }

        return {
            ...createEmptyRootTable(),
            collectionName: pascalCase(selected.sourceTableName),
            sourceTableSchema: selected.sourceTableSchema ?? null,
            sourceTableName: selected.sourceTableName,
        };
    });
}
