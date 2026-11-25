import {
    ConnectionStringKey,
    DatabaseSettingKey,
    DocumentToggleKey,
    OngoingTaskKey,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";
import {
    getDatabaseRecordTypes,
    getDefaultImportOptions,
} from "components/pages/database/tasks/importData/shared/importOptionsUtils";
import { MigrateRavenDbFormData } from "./migrateRavenDbValidation";
import { SourceServerVersion, getMigratableDocumentToggles, isLegacyVersion } from "./migrateVersionSupport";

type DatabaseItemType = Raven.Client.Documents.Smuggler.DatabaseItemType;
type DatabaseRecordItemType = Raven.Client.Documents.Smuggler.DatabaseRecordItemType;
type SingleDatabaseMigrationConfiguration = Raven.Server.Smuggler.Migration.SingleDatabaseMigrationConfiguration;

export interface RestrictedKeys {
    settings: DatabaseSettingKey[];
    tasks: OngoingTaskKey[];
    connectionStrings: ConnectionStringKey[];
}

const DOCUMENT_TOGGLE_ITEM_TYPES: Partial<Record<DocumentToggleKey, DatabaseItemType>> = {
    isIncludeDocuments: "Documents",
    isIncludeConflicts: "Conflicts",
    isIncludeRevisions: "RevisionDocuments",
    isIncludeCompareExchange: "CompareExchange",
    isIncludeCounters: "CounterGroups",
    isIncludeAttachments: "Attachments",
    isIncludeLegacyAttachments: "LegacyAttachments",
    isIncludeTimeSeries: "TimeSeries",
    isIncludeTimeSeriesDeletedRanges: "TimeSeriesDeletedRanges",
    isIncludeSubscriptions: "Subscriptions",
};

export function getDefaultMigrateFormData(isAdminAccessOrAbove: boolean): MigrateRavenDbFormData {
    return {
        ...getDefaultImportOptions(isAdminAccessOrAbove),
        server: {
            serverUrl: "",
            isSkipServerCertificateValidation: false,
            databaseName: "",
            isImportRavenFs: false,
            authenticationMethod: "none",
            userName: "",
            password: "",
            domain: "",
            apiKey: "",
            isEnableBasicAuthenticationOverUnsecuredHttp: false,
        },
    };
}

export function hasAnyMigrateInclude(data: MigrateRavenDbFormData, version: SourceServerVersion): boolean {
    const isLegacy = isLegacyVersion(version);
    if (isLegacy && data.server.isImportRavenFs) {
        return true;
    }

    const { configuration, documents } = data;
    const hasRecordTypes = !isLegacy && getDatabaseRecordTypes(data).some((recordType) => recordType !== "None");

    return (
        getMigratableDocumentToggles(version).some((key) => documents[key]) ||
        configuration.isIncludeIndexes ||
        (!isLegacy && configuration.isIncludeIdentities) ||
        hasRecordTypes
    );
}

export function toMigrateDto(
    data: MigrateRavenDbFormData,
    version: SourceServerVersion,
    restricted: RestrictedKeys
): SingleDatabaseMigrationConfiguration {
    const { server, documents, configuration, processing } = data;
    const isLegacy = isLegacyVersion(version);

    const databaseRecordTypes = isLegacy
        ? []
        : getDatabaseRecordTypes(data, restricted.settings, restricted.tasks, restricted.connectionStrings);

    const operateOnTypes: string[] = [];
    if (!server.isImportRavenFs) {
        if (databaseRecordTypes.length) {
            operateOnTypes.push("DatabaseRecord");
        }
        if (configuration.isIncludeIndexes) {
            operateOnTypes.push("Indexes");
        }
        if (configuration.isIncludeIdentities && !isLegacy) {
            operateOnTypes.push("Identities");
        }
        getMigratableDocumentToggles(version)
            .filter((key) => documents[key])
            .forEach((key) => operateOnTypes.push(DOCUMENT_TOGGLE_ITEM_TYPES[key]));
    }
    if (operateOnTypes.length === 0) {
        operateOnTypes.push("None");
    }

    const isApiKey = server.authenticationMethod === "apiKey";
    const isWindows = server.authenticationMethod === "windows";
    const isHttps = server.serverUrl.toLowerCase().startsWith("https://");

    return {
        ServerUrl: server.serverUrl,
        MigrationSettings: {
            DatabaseName: server.databaseName,
            // the generated types are single literals, while the server accepts comma separated lists
            OperateOnTypes: operateOnTypes.join(",") as DatabaseItemType,
            OperateOnDatabaseRecordTypes: (databaseRecordTypes.length
                ? databaseRecordTypes.join(",")
                : "None") as DatabaseRecordItemType,
            RemoveAnalyzers: configuration.isRemoveAnalyzers,
            ImportRavenFs: server.isImportRavenFs,
            TransformScript: processing.isUseTransformScript ? processing.transformScript : "",
        },
        UserName: isWindows ? server.userName : null,
        Password: isWindows ? server.password : null,
        Domain: isWindows ? server.domain : null,
        ApiKey: isApiKey ? server.apiKey : null,
        EnableBasicAuthenticationOverUnsecuredHttp: isApiKey && server.isEnableBasicAuthenticationOverUnsecuredHttp,
        BuildMajorVersion: version.majorVersion,
        BuildVersion: version.buildVersion,
        SkipServerCertificateValidation: isHttps && server.isSkipServerCertificateValidation,
    };
}
