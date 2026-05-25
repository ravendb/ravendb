import { SelectOption } from "components/common/select/Select";
import {
    FormLinkedTable,
    FormRootTable,
    FormRootTableColumn,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { camelCase, upperFirst } from "lodash";

import CdcSinkSchema = Raven.Client.Documents.Operations.CdcSink.Schema;

export type CdcSinkSourceSchema = CdcSinkSchema.CdcSinkSourceSchema;
export type CdcSinkSourceTable = CdcSinkSchema.CdcSinkSourceTable;
export type CdcSinkSourceForeignKey = CdcSinkSourceTable["ForeignKeys"][number];

export interface CdcSinkSourceTableOption extends SelectOption<string> {
    table?: CdcSinkSourceTable;
}

export const pascalCase = (name: string) => upperFirst(camelCase(name));

export const propertyNameFromJoinColumn = (name: string) => pascalCase(name.endsWith("_id") ? name.slice(0, -3) : name);

export function isTableSupported(table: CdcSinkSourceTable) {
    return table.IsCdcEnabled && table.UnsupportedReason == null;
}

export function isColumnSupported(column: CdcSinkSchema.CdcSinkSourceColumn) {
    return column.IsCdcCapturable && column.UnsupportedReason == null;
}

export function mapSourceColumnsToFormData(table: CdcSinkSourceTable): FormRootTableColumn[] {
    return table.Columns.filter(isColumnSupported).map(
        (x): FormRootTableColumn => ({
            column: x.Name,
            name: pascalCase(x.Name),
            type: x.SuggestedType,
        })
    );
}

export function stringValues(values: string[]) {
    return values.map((value) => ({ value }));
}

export function getSourceTableOptionValue(table: CdcSinkSourceTable) {
    return table.SourceTableName;
}

export function getSourceTableOptionLabel(table: CdcSinkSourceTable) {
    return `${table.SourceTableSchema}.${table.SourceTableName}`;
}

export function getSourceTableOptions(
    schema: CdcSinkSourceSchema,
    sourceTableSchema: string,
    excludedTable?: { sourceTableSchema: string; sourceTableName: string }
): CdcSinkSourceTableOption[] {
    const schemaFilter = sourceTableSchema?.trim();

    return (schema?.Tables ?? [])
        .filter(isTableSupported)
        .filter((table) => !schemaFilter || table.SourceTableSchema === schemaFilter)
        .filter(
            (table) =>
                !excludedTable ||
                table.SourceTableSchema !== excludedTable.sourceTableSchema ||
                table.SourceTableName !== excludedTable.sourceTableName
        )
        .map((table) => ({
            value: getSourceTableOptionValue(table),
            label: getSourceTableOptionLabel(table),
            table,
        }));
}

export function getSourceSchemaOptions(schema: CdcSinkSourceSchema): SelectOption<string>[] {
    const schemas = new Set((schema?.Tables ?? []).filter(isTableSupported).map((table) => table.SourceTableSchema));

    return Array.from(schemas)
        .sort()
        .map((sourceSchema) => ({
            value: sourceSchema,
            label: sourceSchema,
        }));
}

export function findSourceTable(schema: CdcSinkSourceSchema, sourceTableSchema: string, sourceTableName: string) {
    return (schema?.Tables ?? []).find(
        (table) => table.SourceTableSchema === sourceTableSchema && table.SourceTableName === sourceTableName
    );
}

export function getForeignKeysToTable(sourceTable: CdcSinkSourceTable, targetTable: CdcSinkSourceTable) {
    return (sourceTable?.ForeignKeys ?? []).filter(
        (foreignKey) =>
            foreignKey.ReferencedSchema === targetTable.SourceTableSchema &&
            foreignKey.ReferencedTable === targetTable.SourceTableName
    );
}

export function mapSqlTableToFormData(table: CdcSinkSourceTable): FormRootTable {
    const linkedTables = table.ForeignKeys.map(
        (x): FormLinkedTable => ({
            propertyName: x.Columns.map(propertyNameFromJoinColumn).join("And"),
            joinColumns: stringValues(x.Columns),
            linkedCollectionName: pascalCase(x.ReferencedTable),
            sourceTableName: x.ReferencedTable,
            sourceTableSchema: x.ReferencedSchema,
        })
    );

    return {
        collectionName: pascalCase(table.SourceTableName),
        columns: mapSourceColumnsToFormData(table),
        disabled: false,
        embeddedTables: [],
        linkedTables,
        onDelete: { ignoreDeletes: false, patch: "" },
        patch: "",
        primaryKeyColumns: stringValues(table.PrimaryKeyColumns),
        sourceTableName: table.SourceTableName,
        sourceTableSchema: table.SourceTableSchema,
    } satisfies FormRootTable;
}
