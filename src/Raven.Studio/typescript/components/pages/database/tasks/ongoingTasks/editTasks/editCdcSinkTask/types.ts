export type CdcSinkColumnType = "Default" | "Json" | "Attachment";

export interface CdcSinkColumnFormData {
    column: string;
    name: string;
    type: CdcSinkColumnType;
}

export interface CdcSinkFormData {
    name: string;
    connectionStringName: string;
    isSetResponsibleNode: boolean;
    responsibleNode: string;
    disabled: boolean;
    tables: CdcSinkTableFormData[];
}

export interface CdcSinkTableFormData {
    name: string;
    sourceTableSchema: string;
    sourceTableName: string;
    columns: CdcSinkColumnFormData[];
    primaryKeyColumns: string[];
    patch: string;
    onDelete: CdcSinkOnDeleteFormData | null;
    disabled: boolean;
    embeddedTables: CdcSinkEmbeddedTableFormData[];
    linkedTables: CdcSinkLinkedTableFormData[];
}

export interface CdcSinkEmbeddedTableFormData {
    sourceTableSchema: string;
    sourceTableName: string;
    propertyName: string;
    type: "Array" | "Map" | "Value";
    joinColumns: string[];
    primaryKeyColumns: string[];
    columns: CdcSinkColumnFormData[];
    patch: string;
    onDelete: CdcSinkOnDeleteFormData | null;
    caseSensitiveKeys: boolean;
    embeddedTables: CdcSinkEmbeddedTableFormData[];
}

export interface CdcSinkLinkedTableFormData {
    sourceTableSchema: string;
    sourceTableName: string;
    propertyName: string;
    linkedCollectionName: string;
    type: "Array" | "Value";
    joinColumns: string[];
}

export interface CdcSinkOnDeleteFormData {
    patch: string;
    ignoreDeletes: boolean;
}
