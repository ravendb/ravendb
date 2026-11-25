import {
    ConnectionStringKey,
    DatabaseSettingKey,
    OngoingTaskKey,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";
import { ImportFromFileFormData } from "./importFromFileValidation";
import {
    getDatabaseRecordTypes,
    getDefaultImportOptions,
} from "components/pages/database/tasks/importData/shared/importOptionsUtils";
import endpoints = require("endpoints");
import appUrl = require("common/appUrl");

type DatabaseItemType = Raven.Client.Documents.Smuggler.DatabaseItemType;
type ImportOptions = Raven.Client.Documents.Smuggler.DatabaseSmugglerImportOptions;

export type ImportCommandType = "PowerShell" | "Cmd" | "Bash";

export function getDefaultFormData(isAdminAccessOrAbove: boolean): ImportFromFileFormData {
    const importOptions = getDefaultImportOptions(isAdminAccessOrAbove);
    return {
        file: null,
        ...importOptions,
        collections: {
            isImportAllCollections: true,
            includedCollections: [],
        },
        processing: {
            ...importOptions.processing,
            isSetMaxReadOpsPerSecond: false,
            maxReadOpsPerSecond: null,
            isEncrypted: false,
            encryptionKey: "",
        },
    };
}

export function toImportDto(
    formData: ImportFromFileFormData,
    restrictedSettingKeys: DatabaseSettingKey[] = [],
    restrictedOngoingTaskKeys: OngoingTaskKey[] = [],
    restrictedConnectionStringKeys: ConnectionStringKey[] = []
): ImportOptions {
    const { documents, collections, configuration, processing } = formData;

    const operateOnTypes: DatabaseItemType[] = [];

    const databaseRecordTypes = getDatabaseRecordTypes(
        formData,
        restrictedSettingKeys,
        restrictedOngoingTaskKeys,
        restrictedConnectionStringKeys
    );

    if (databaseRecordTypes.length) {
        operateOnTypes.push("DatabaseRecord");
    }
    if (documents.isIncludeDocuments) {
        operateOnTypes.push("Documents");
    }
    if (documents.isIncludeConflicts) {
        operateOnTypes.push("Conflicts");
    }
    if (configuration.isIncludeIndexes) {
        operateOnTypes.push("Indexes");
    }
    if (documents.isIncludeRevisions) {
        operateOnTypes.push("RevisionDocuments");
    }
    if (configuration.isIncludeIdentities) {
        operateOnTypes.push("Identities");
    }
    if (documents.isIncludeCompareExchange) {
        operateOnTypes.push("CompareExchange");
    }
    if (documents.isIncludeCounters) {
        operateOnTypes.push("CounterGroups");
    }
    if (documents.isIncludeAttachments) {
        operateOnTypes.push("Attachments");
    }
    if (documents.isIncludeLegacyAttachments) {
        operateOnTypes.push("LegacyAttachments");
    }
    if (documents.isIncludeTimeSeries) {
        operateOnTypes.push("TimeSeries");
    }
    if (documents.isIncludeTimeSeriesDeletedRanges) {
        operateOnTypes.push("TimeSeriesDeletedRanges");
    }
    if (documents.isIncludeSubscriptions) {
        operateOnTypes.push("Subscriptions");
    }
    if (documents.isIncludeDocumentsTombstones) {
        operateOnTypes.push("Tombstones");
    }
    if (documents.isIncludeCompareExchangeTombstones) {
        operateOnTypes.push("CompareExchangeTombstones");
    }

    return {
        IncludeExpired: documents.isIncludeExpiredDocuments,
        IncludeArtificial: documents.isIncludeArtificialDocuments,
        IncludeArchived: documents.isIncludeArchivedDocuments,
        TransformScript: processing.isUseTransformScript ? processing.transformScript : "",
        RemoveAnalyzers: configuration.isRemoveAnalyzers,
        EncryptionKey: processing.isEncrypted ? processing.encryptionKey : undefined,
        OperateOnTypes: operateOnTypes.join(","),
        OperateOnDatabaseRecordTypes: databaseRecordTypes.length ? databaseRecordTypes.join(",") : undefined,
        Collections: collections.isImportAllCollections ? null : collections.includedCollections,
        MaxReadOpsPerSecond: processing.isSetMaxReadOpsPerSecond ? processing.maxReadOpsPerSecond : null,
    } as ImportOptions;
}

export function hasAnyInclude(
    formData: Pick<ImportFromFileFormData, "documents" | "configuration">,
    restrictedSettingKeys: DatabaseSettingKey[] = [],
    restrictedOngoingTaskKeys: OngoingTaskKey[] = [],
    restrictedConnectionStringKeys: ConnectionStringKey[] = []
): boolean {
    const d = formData.documents;
    const c = formData.configuration;
    return (
        d.isIncludeDocuments ||
        d.isIncludeAttachments ||
        d.isIncludeCounters ||
        d.isIncludeRevisions ||
        d.isIncludeTimeSeries ||
        d.isIncludeTimeSeriesDeletedRanges ||
        d.isIncludeArtificialDocuments ||
        d.isIncludeArchivedDocuments ||
        d.isIncludeConflicts ||
        d.isIncludeCompareExchange ||
        d.isIncludeLegacyAttachments ||
        d.isIncludeDocumentsTombstones ||
        d.isIncludeCompareExchangeTombstones ||
        d.isIncludeSubscriptions ||
        c.isIncludeIndexes ||
        c.isIncludeIdentities ||
        getDatabaseRecordTypes(
            formData,
            restrictedSettingKeys,
            restrictedOngoingTaskKeys,
            restrictedConnectionStringKeys
        ).length > 0
    );
}

export function buildImportCurlCommand(
    commandType: ImportCommandType,
    formData: ImportFromFileFormData,
    databaseName: string,
    restrictedSettingKeys: DatabaseSettingKey[] = [],
    restrictedOngoingTaskKeys: OngoingTaskKey[] = [],
    restrictedConnectionStringKeys: ConnectionStringKey[] = []
): string {
    const dto = toImportDto(formData, restrictedSettingKeys, restrictedOngoingTaskKeys, restrictedConnectionStringKeys);
    if (!dto.TransformScript) {
        delete dto.TransformScript;
    }
    const json = JSON.stringify(dto);
    const fileName = formData.file?.name || "Dump of Database.ravendbdump";
    const commandEndpointUrl =
        appUrl.forServer() + appUrl.forDatabaseQuery(databaseName) + endpoints.databases.smuggler.smugglerImport;

    switch (commandType) {
        case "PowerShell":
            return `curl.exe -F 'importOptions=${json.replace(/"/g, '\\"')}' -F 'file=@.\\${fileName}' ${commandEndpointUrl}`;
        case "Cmd":
            return `curl.exe -F "importOptions=${json.replace(/"/g, '\\"')}" -F "file=@.\\${fileName}" ${commandEndpointUrl}`;
        case "Bash":
            return `curl -F 'importOptions=${json}' -F 'file=@${fileName}' ${commandEndpointUrl}`;
    }
}
