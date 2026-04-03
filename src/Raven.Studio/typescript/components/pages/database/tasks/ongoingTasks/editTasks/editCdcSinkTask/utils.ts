import {
    CdcSinkFormData,
    CdcSinkTableFormData,
    CdcSinkEmbeddedTableFormData,
    CdcSinkLinkedTableFormData,
    CdcSinkOnDeleteFormData,
    CdcSinkColumnFormData,
} from "./types";

type CdcSinkConfiguration = Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration;
type CdcSinkTableConfig = Raven.Client.Documents.Operations.CdcSink.CdcSinkTableConfig;
type CdcSinkEmbeddedTableConfig = Raven.Client.Documents.Operations.CdcSink.CdcSinkEmbeddedTableConfig;
type CdcSinkLinkedTableConfig = Raven.Client.Documents.Operations.CdcSink.CdcSinkLinkedTableConfig;
type CdcSinkOnDeleteConfig = Raven.Client.Documents.Operations.CdcSink.CdcSinkOnDeleteConfig;
type CdcColumnMapping = Raven.Client.Documents.Operations.CdcSink.CdcColumnMapping;

function mapOnDeleteFromDto(dto: CdcSinkOnDeleteConfig | null | undefined): CdcSinkOnDeleteFormData | null {
    if (!dto) {
        return null;
    }
    return {
        patch: dto.Patch ?? "",
        ignoreDeletes: dto.IgnoreDeletes ?? false,
    };
}

function mapColumnsFromDto(dtoColumns: CdcColumnMapping[] | null | undefined): CdcSinkColumnFormData[] {
    return (dtoColumns ?? []).map((c) => ({
        column: c.Column ?? "",
        name: c.Name ?? "",
        type: c.Type ?? "Default",
    }));
}

function mapEmbeddedTableFromDto(dto: CdcSinkEmbeddedTableConfig): CdcSinkEmbeddedTableFormData {
    return {
        sourceTableSchema: dto.SourceTableSchema ?? "",
        sourceTableName: dto.SourceTableName ?? "",
        propertyName: dto.PropertyName ?? "",
        type: dto.Type ?? "Array",
        joinColumns: dto.JoinColumns ?? [],
        primaryKeyColumns: dto.PrimaryKeyColumns ?? [],
        columns: mapColumnsFromDto(dto.Columns),
        patch: dto.Patch ?? "",
        onDelete: mapOnDeleteFromDto(dto.OnDelete),
        caseSensitiveKeys: dto.CaseSensitiveKeys ?? false,
        embeddedTables: (dto.EmbeddedTables ?? []).map(mapEmbeddedTableFromDto),
    };
}

function mapLinkedTableFromDto(dto: CdcSinkLinkedTableConfig): CdcSinkLinkedTableFormData {
    return {
        sourceTableSchema: dto.SourceTableSchema ?? "",
        sourceTableName: dto.SourceTableName ?? "",
        propertyName: dto.PropertyName ?? "",
        linkedCollectionName: dto.LinkedCollectionName ?? "",
        type: (dto.Type === "Array" ? "Array" : "Value") as "Array" | "Value",
        joinColumns: dto.JoinColumns ?? [],
    };
}

function mapTableFromDto(dto: CdcSinkTableConfig): CdcSinkTableFormData {
    return {
        name: dto.Name ?? "",
        sourceTableSchema: dto.SourceTableSchema ?? "",
        sourceTableName: dto.SourceTableName ?? "",
        columns: mapColumnsFromDto(dto.Columns),
        primaryKeyColumns: dto.PrimaryKeyColumns ?? [],
        patch: dto.Patch ?? "",
        onDelete: mapOnDeleteFromDto(dto.OnDelete),
        disabled: dto.Disabled ?? false,
        embeddedTables: (dto.EmbeddedTables ?? []).map(mapEmbeddedTableFromDto),
        linkedTables: (dto.LinkedTables ?? []).map(mapLinkedTableFromDto),
    };
}

const getDefaultValues = (dto?: CdcSinkConfiguration): CdcSinkFormData => {
    if (!dto) {
        return {
            name: "",
            connectionStringName: "",
            isSetResponsibleNode: false,
            responsibleNode: "",
            disabled: false,
            tables: [],
        };
    }

    return {
        name: dto.Name ?? "",
        connectionStringName: dto.ConnectionStringName ?? "",
        isSetResponsibleNode: dto.MentorNode != null,
        responsibleNode: dto.MentorNode ?? "",
        disabled: dto.Disabled ?? false,
        tables: (dto.Tables ?? []).map(mapTableFromDto),
    };
};

function mapOnDeleteToDto(data: CdcSinkOnDeleteFormData | null): CdcSinkOnDeleteConfig | null {
    if (!data) {
        return null;
    }
    return {
        Patch: data.patch || null,
        IgnoreDeletes: data.ignoreDeletes,
    };
}

function mapColumnsToDto(columns: CdcSinkColumnFormData[]): CdcColumnMapping[] {
    return columns.map((c) => ({
        Column: c.column,
        Name: c.name,
        Type: c.type,
    }));
}

function mapEmbeddedTableToDto(data: CdcSinkEmbeddedTableFormData): CdcSinkEmbeddedTableConfig {
    return {
        SourceTableSchema: data.sourceTableSchema || null,
        SourceTableName: data.sourceTableName,
        PropertyName: data.propertyName,
        Type: data.type,
        JoinColumns: data.joinColumns,
        PrimaryKeyColumns: data.primaryKeyColumns,
        Columns: mapColumnsToDto(data.columns),
        Patch: data.patch || null,
        OnDelete: mapOnDeleteToDto(data.onDelete),
        CaseSensitiveKeys: data.caseSensitiveKeys,
        EmbeddedTables: data.embeddedTables.map(mapEmbeddedTableToDto),
    };
}

function mapLinkedTableToDto(data: CdcSinkLinkedTableFormData): CdcSinkLinkedTableConfig {
    return {
        SourceTableSchema: data.sourceTableSchema || null,
        SourceTableName: data.sourceTableName,
        PropertyName: data.propertyName,
        LinkedCollectionName: data.linkedCollectionName,
        Type: data.type,
        JoinColumns: data.joinColumns,
    };
}

function mapTableToDto(data: CdcSinkTableFormData): CdcSinkTableConfig {
    return {
        Name: data.name,
        SourceTableSchema: data.sourceTableSchema || null,
        SourceTableName: data.sourceTableName,
        Columns: mapColumnsToDto(data.columns),
        PrimaryKeyColumns: data.primaryKeyColumns,
        Patch: data.patch || null,
        OnDelete: mapOnDeleteToDto(data.onDelete),
        Disabled: data.disabled,
        EmbeddedTables: data.embeddedTables.map(mapEmbeddedTableToDto),
        LinkedTables: data.linkedTables.map(mapLinkedTableToDto),
    };
}

const mapToDto = (data: CdcSinkFormData, taskId?: number): CdcSinkConfiguration => {
    return {
        TaskId: taskId ?? 0,
        Name: data.name,
        ConnectionStringName: data.connectionStringName,
        Disabled: data.disabled,
        MentorNode: data.isSetResponsibleNode ? data.responsibleNode : null,
        PinToMentorNode: false,
        Tables: data.tables.map(mapTableToDto),
        Postgres: null,
        SkipInitialLoad: false,
    };
};

export const editCdcSinkTaskUtils = {
    getDefaultValues,
    mapToDto,
};
