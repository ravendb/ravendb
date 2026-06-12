import { api } from "@/api/api";
import type {
    CdcColumnMapping,
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkOnDeleteConfig,
    CdcSinkTableConfig,
} from "@/api/generated/server-api";
import type {
    FormColumnMapping,
    FormEmbeddedTable,
    FormLinkedTable,
    FormRootTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { toStringValues } from "@/lib/form-utils";
import { getSourceTableLabel } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useFormContext } from "react-hook-form";

export function useMapTablesStep() {
    const { getValues, setValue } = useFormContext<AppFormData>();

    return async () => {
        const formTables = getValues("mapTables.tables");

        await api.services.setup.map({
            tables: formTables.map(mapTableToDto),
        });

        const firstTable = formTables[0];
        setValue("preview.table", getSourceTableLabel(firstTable) ?? "");
    };
}

function mapTableToDto(table: FormRootTable): CdcSinkTableConfig {
    return {
        collectionName: table.collectionName,
        columns: (table.columns ?? []).map(mapColumnToDto),
        disabled: table.disabled,
        embeddedTables: (table.embeddedTables ?? []).map(mapEmbeddedTableToDto),
        linkedTables: (table.linkedTables ?? []).map(mapLinkedTableToDto),
        onDelete: mapOnDeleteToDto(table.onDelete),
        patch: table.patch || null,
        primaryKeyColumns: toStringValues(table.primaryKeyColumns),
        sourceTableName: table.sourceTableName,
        sourceTableSchema: table.sourceTableSchema,
    };
}

function mapEmbeddedTableToDto(table: FormEmbeddedTable): CdcSinkEmbeddedTableConfig {
    return {
        caseSensitiveKeys: table.caseSensitiveKeys,
        columns: (table.columns ?? []).map(mapColumnToDto),
        embeddedTables: (table.embeddedTables ?? []).map(mapEmbeddedTableToDto),
        joinColumns: toStringValues(table.joinColumns),
        linkedTables: (table.linkedTables ?? []).map(mapLinkedTableToDto),
        onDelete: mapOnDeleteToDto(table.onDelete),
        patch: table.patch || null,
        primaryKeyColumns: toStringValues(table.primaryKeyColumns),
        propertyName: table.propertyName,
        sourceTableName: table.sourceTableName,
        sourceTableSchema: table.sourceTableSchema,
        type: table.type,
    };
}

function mapLinkedTableToDto(table: FormLinkedTable): CdcSinkLinkedTableConfig {
    return {
        joinColumns: toStringValues(table.joinColumns),
        linkedCollectionName: table.linkedCollectionName,
        propertyName: table.propertyName,
        sourceTableName: table.sourceTableName,
        sourceTableSchema: table.sourceTableSchema,
    };
}

function mapColumnToDto(column: FormColumnMapping): CdcColumnMapping {
    return {
        column: column.column,
        name: column.name,
        type: column.type,
    };
}

function mapOnDeleteToDto(onDelete: FormRootTable["onDelete"]): CdcSinkOnDeleteConfig {
    return {
        ignoreDeletes: onDelete?.ignoreDeletes ?? false,
        patch: onDelete?.patch || null,
    };
}
