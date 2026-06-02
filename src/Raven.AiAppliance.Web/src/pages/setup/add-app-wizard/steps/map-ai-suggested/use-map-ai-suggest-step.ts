import { api } from "@/api/api";
import type {
    CdcColumnMapping,
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkOnDeleteConfig,
    CdcSinkTableConfig,
} from "@/api/generated/server-api";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useMutation } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";

export function useMapAiSuggestStep() {
    const { getValues } = useFormContext<AppFormData>();

    const connectAndDiscover = useMutation({
        mutationFn: async () => {
            const formTables = getValues("mapAiSuggest.tables");

            await api.services.setup.map({
                tables: formTables.map(mapTableToDto),
            });

            return true;
        },
    });

    return connectAndDiscover;
}

// TODO move mappings to utils file or adjust schema type to match API

function mapTableToDto(table: AppFormData["mapAiSuggest"]["tables"][0]): CdcSinkTableConfig {
    return {
        collectionName: table.collectionName,
        columns: (table.columns ?? []).map(mapColumnToDto),
        disabled: table.disabled,
        embeddedTables: (table.embeddedTables ?? []).map(mapEmbeddedTableToDto),
        linkedTables: (table.linkedTables ?? []).map(mapLinkedTableToDto),
        onDelete: mapOnDeleteToDto(table.onDelete),
        patch: table.patch,
        primaryKeyColumns: table.primaryKeyColumns,
        sourceTableName: table.sourceTableName,
        sourceTableSchema: table.sourceTableSchema,
    };
}

function mapEmbeddedTableToDto(
    table: AppFormData["mapAiSuggest"]["tables"][0]["embeddedTables"][0],
): CdcSinkEmbeddedTableConfig {
    return {
        caseSensitiveKeys: table.caseSensitiveKeys,
        columns: (table.columns ?? []).map(mapColumnToDto),
        embeddedTables: (table.embeddedTables ?? []).map(mapEmbeddedTableToDto),
        joinColumns: table.joinColumns,
        linkedTables: (table.linkedTables ?? []).map(mapLinkedTableToDto),
        onDelete: mapOnDeleteToDto(table.onDelete),
        patch: table.patch,
        primaryKeyColumns: table.primaryKeyColumns,
        propertyName: table.propertyName,
        sourceTableName: table.sourceTableName,
        sourceTableSchema: table.sourceTableSchema,
        type: table.type,
    };
}

function mapLinkedTableToDto(
    table: AppFormData["mapAiSuggest"]["tables"][0]["linkedTables"][0],
): CdcSinkLinkedTableConfig {
    return {
        joinColumns: table.joinColumns,
        linkedCollectionName: table.linkedCollectionName,
        propertyName: table.propertyName,
        sourceTableName: table.sourceTableName,
        sourceTableSchema: table.sourceTableSchema,
    };
}

function mapColumnToDto(column: AppFormData["mapAiSuggest"]["tables"][0]["columns"][0]): CdcColumnMapping {
    return {
        column: column.column,
        name: column.name,
        type: column.type,
    };
}

function mapOnDeleteToDto(onDelete: AppFormData["mapAiSuggest"]["tables"][0]["onDelete"]): CdcSinkOnDeleteConfig {
    return {
        ignoreDeletes: onDelete?.ignoreDeletes ?? false,
        patch: onDelete?.patch || null,
    };
}
