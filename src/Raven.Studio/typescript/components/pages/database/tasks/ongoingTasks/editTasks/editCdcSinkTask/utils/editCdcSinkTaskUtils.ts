import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";

import CdcSink = Raven.Client.Documents.Operations.CdcSink;
type EditCdcSinkTaskTable = NonNullable<EditCdcSinkTaskFormData["tables"]>[number];
type EditCdcSinkTaskColumnMapping = NonNullable<EditCdcSinkTaskTable["columns"]>[number];
type EditCdcSinkTaskEmbeddedTable = NonNullable<EditCdcSinkTaskTable["embeddedTables"]>[number];
type EditCdcSinkTaskLinkedTable = NonNullable<EditCdcSinkTaskTable["linkedTables"]>[number];
type EditCdcSinkTaskOnDelete = EditCdcSinkTaskTable["onDelete"];
type StringValueItem = NonNullable<EditCdcSinkTaskTable["primaryKeyColumns"]>[number];

function mapFromDto(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskCdcSink): EditCdcSinkTaskFormData {
    if (!dto) {
        return {
            name: "",
            state: "Enabled",
            isSetResponsibleNode: false,
            responsibleNode: "",
            isPinResponsibleNode: false,
            connectionStringName: "",
            skipInitialLoad: false,
            postgresPublicationName: "",
            postgresSlotName: "",
            tables: [],
        };
    }

    const configuration = dto.Configuration;

    return {
        name: configuration.Name,
        state: configuration.Disabled ? "Disabled" : "Enabled",
        isSetResponsibleNode: Boolean(configuration.MentorNode),
        responsibleNode: configuration.MentorNode ?? "",
        isPinResponsibleNode: configuration.PinToMentorNode ?? false,
        connectionStringName: configuration.ConnectionStringName ?? "",
        skipInitialLoad: configuration.SkipInitialLoad ?? false,
        postgresPublicationName: configuration.Postgres?.PublicationName ?? "",
        postgresSlotName: configuration.Postgres?.SlotName ?? "",
        tables: (configuration.Tables ?? []).map(mapTableFromDto),
    };
}

function mapToDto(formData: EditCdcSinkTaskFormData, taskId: number): CdcSink.CdcSinkConfiguration {
    const hasPostgresSettings = formData.postgresPublicationName || formData.postgresSlotName;
    const postgresSettings: CdcSink.CdcSinkConfiguration["Postgres"] = hasPostgresSettings
        ? {
              PublicationName: formData.postgresPublicationName,
              SlotName: formData.postgresSlotName,
          }
        : null;

    return {
        TaskId: taskId,
        Name: formData.name,
        Disabled: formData.state === "Disabled",
        ConnectionStringName: formData.connectionStringName,
        MentorNode: formData.isSetResponsibleNode ? formData.responsibleNode : null,
        PinToMentorNode: formData.isPinResponsibleNode,
        Postgres: postgresSettings,
        SkipInitialLoad: formData.skipInitialLoad,
        Tables: (formData.tables ?? []).map(mapTableToDto),
    };
}

function mapTableFromDto(table: CdcSink.CdcSinkTableConfig): EditCdcSinkTaskTable {
    return {
        collectionName: table.CollectionName ?? "",
        columns: (table.Columns ?? []).map(mapColumnFromDto),
        disabled: table.Disabled ?? false,
        embeddedTables: (table.EmbeddedTables ?? []).map(mapEmbeddedTableFromDto),
        linkedTables: (table.LinkedTables ?? []).map(mapLinkedTableFromDto),
        onDelete: mapOnDeleteFromDto(table.OnDelete),
        patch: table.Patch ?? "",
        primaryKeyColumns: mapStringArrayFromDto(table.PrimaryKeyColumns),
        sourceTableName: table.SourceTableName ?? "",
        sourceTableSchema: table.SourceTableSchema ?? "",
    };
}

function mapEmbeddedTableFromDto(table: CdcSink.CdcSinkEmbeddedTableConfig): EditCdcSinkTaskEmbeddedTable {
    return {
        caseSensitiveKeys: table.CaseSensitiveKeys ?? false,
        columns: (table.Columns ?? []).map(mapColumnFromDto),
        embeddedTables: (table.EmbeddedTables ?? []).map(mapEmbeddedTableFromDto),
        joinColumns: mapStringArrayFromDto(table.JoinColumns),
        linkedTables: (table.LinkedTables ?? []).map(mapLinkedTableFromDto),
        onDelete: mapOnDeleteFromDto(table.OnDelete),
        patch: table.Patch ?? "",
        primaryKeyColumns: mapStringArrayFromDto(table.PrimaryKeyColumns),
        propertyName: table.PropertyName ?? "",
        sourceTableName: table.SourceTableName ?? "",
        sourceTableSchema: table.SourceTableSchema ?? "",
        type: table.Type ?? "Array",
    };
}

function mapLinkedTableFromDto(table: CdcSink.CdcSinkLinkedTableConfig): EditCdcSinkTaskLinkedTable {
    return {
        joinColumns: mapStringArrayFromDto(table.JoinColumns),
        linkedCollectionName: table.LinkedCollectionName ?? "",
        propertyName: table.PropertyName ?? "",
        sourceTableName: table.SourceTableName ?? "",
        sourceTableSchema: table.SourceTableSchema ?? "",
    };
}

function mapColumnFromDto(column: CdcSink.CdcColumnMapping): EditCdcSinkTaskColumnMapping {
    return {
        column: column.Column ?? "",
        name: column.Name ?? "",
        type: column.Type ?? "Default",
    };
}

function mapOnDeleteFromDto(onDelete: CdcSink.CdcSinkOnDeleteConfig): EditCdcSinkTaskOnDelete {
    return {
        ignoreDeletes: onDelete?.IgnoreDeletes ?? false,
        patch: onDelete?.Patch ?? "",
    };
}

function mapStringArrayFromDto(values: string[]): StringValueItem[] {
    return (values ?? []).map((value) => ({ value }));
}

function mapTableToDto(table: EditCdcSinkTaskTable): CdcSink.CdcSinkTableConfig {
    return {
        CollectionName: table.collectionName ?? "",
        Columns: (table.columns ?? []).map(mapColumnToDto),
        Disabled: table.disabled,
        EmbeddedTables: (table.embeddedTables ?? []).map(mapEmbeddedTableToDto),
        LinkedTables: (table.linkedTables ?? []).map(mapLinkedTableToDto),
        OnDelete: mapOnDeleteToDto(table.onDelete),
        Patch: table.patch || null,
        PrimaryKeyColumns: mapStringArrayToDto(table.primaryKeyColumns),
        SourceTableName: table.sourceTableName ?? "",
        SourceTableSchema: table.sourceTableSchema || null,
    };
}

function mapEmbeddedTableToDto(table: EditCdcSinkTaskEmbeddedTable): CdcSink.CdcSinkEmbeddedTableConfig {
    return {
        CaseSensitiveKeys: table.caseSensitiveKeys,
        Columns: (table.columns ?? []).map(mapColumnToDto),
        EmbeddedTables: (table.embeddedTables ?? []).map(mapEmbeddedTableToDto),
        JoinColumns: mapStringArrayToDto(table.joinColumns),
        LinkedTables: (table.linkedTables ?? []).map(mapLinkedTableToDto),
        OnDelete: mapOnDeleteToDto(table.onDelete),
        Patch: table.patch || null,
        PrimaryKeyColumns: mapStringArrayToDto(table.primaryKeyColumns),
        PropertyName: table.propertyName ?? "",
        SourceTableName: table.sourceTableName ?? "",
        SourceTableSchema: table.sourceTableSchema || null,
        Type: table.type ?? "Array",
    };
}

function mapLinkedTableToDto(table: EditCdcSinkTaskLinkedTable): CdcSink.CdcSinkLinkedTableConfig {
    return {
        JoinColumns: mapStringArrayToDto(table.joinColumns),
        LinkedCollectionName: table.linkedCollectionName ?? "",
        PropertyName: table.propertyName ?? "",
        SourceTableName: table.sourceTableName ?? "",
        SourceTableSchema: table.sourceTableSchema || null,
    };
}

function mapColumnToDto(column: EditCdcSinkTaskColumnMapping): CdcSink.CdcColumnMapping {
    return {
        Column: column.column ?? "",
        Name: column.name ?? "",
        Type: column.type ?? "Default",
    };
}

function mapOnDeleteToDto(onDelete: EditCdcSinkTaskOnDelete): CdcSink.CdcSinkOnDeleteConfig {
    return {
        IgnoreDeletes: onDelete?.ignoreDeletes ?? false,
        Patch: onDelete?.patch || null,
    };
}

function mapStringArrayToDto(values: StringValueItem[]): string[] {
    return (values ?? []).map((item) => item.value).filter(Boolean);
}

export const editCdcSinkTaskUtils = {
    mapFromDto,
    mapToDto,
};
