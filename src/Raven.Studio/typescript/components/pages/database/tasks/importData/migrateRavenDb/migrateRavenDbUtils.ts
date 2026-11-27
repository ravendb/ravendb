import {
    ConnectionStringKey,
    DatabaseSettingKey,
    OngoingTaskKey,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";
import {
    getDatabaseRecordTypes,
    getDefaultImportOptions,
} from "components/pages/database/tasks/importData/shared/importOptionsUtils";
import { MigrateRavenDbFormData } from "components/pages/database/tasks/importData/migrateRavenDb/migrateRavenDbValidation";
import {
    MigrateDocumentToggleKey,
    SourceServerVersion,
    getEffectiveAuthenticationMethod,
    getMigratableDocumentToggles,
    isLegacyVersion,
} from "components/pages/database/tasks/importData/migrateRavenDb/migrateVersionSupport";

type DatabaseItemType = Raven.Client.Documents.Smuggler.DatabaseItemType;
type DatabaseRecordItemType = Raven.Client.Documents.Smuggler.DatabaseRecordItemType;
type SingleDatabaseMigrationConfiguration = Raven.Server.Smuggler.Migration.SingleDatabaseMigrationConfiguration;

export interface RestrictedKeys {
    settings: DatabaseSettingKey[];
    tasks: OngoingTaskKey[];
    connectionStrings: ConnectionStringKey[];
}

const DOCUMENT_TOGGLE_ITEM_TYPES: Record<MigrateDocumentToggleKey, DatabaseItemType> = {
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

function getMigrateRecordTypes(
    data: Pick<MigrateRavenDbFormData, "configuration">,
    version: SourceServerVersion,
    restricted: RestrictedKeys
): DatabaseRecordItemType[] {
    if (isLegacyVersion(version)) {
        return [];
    }
    return getDatabaseRecordTypes(data, restricted.settings, restricted.tasks, restricted.connectionStrings);
}

export function getDefaultMigrateFormData(isAdminAccessOrAbove: boolean): MigrateRavenDbFormData {
    const importOptions = getDefaultImportOptions(isAdminAccessOrAbove);
    return {
        ...importOptions,
        documents: { ...importOptions.documents, isIncludeLegacyAttachments: true },
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

export function hasAnyMigrateInclude(
    data: Pick<MigrateRavenDbFormData, "server" | "documents" | "configuration">,
    version: SourceServerVersion,
    restricted: RestrictedKeys
): boolean {
    const isLegacy = isLegacyVersion(version);
    if (isLegacy && data.server.isImportRavenFs) {
        return true;
    }

    const { configuration, documents } = data;

    return (
        getMigratableDocumentToggles(version).some((key) => documents[key]) ||
        configuration.isIncludeIndexes ||
        (!isLegacy && configuration.isIncludeIdentities) ||
        getMigrateRecordTypes(data, version, restricted).length > 0
    );
}

export function toMigrateDto(
    data: MigrateRavenDbFormData,
    version: SourceServerVersion,
    restricted: RestrictedKeys
): SingleDatabaseMigrationConfiguration {
    const { server, documents, configuration, processing } = data;
    const isLegacy = isLegacyVersion(version);
    const isImportRavenFs = isLegacy && server.isImportRavenFs;

    const databaseRecordTypes = getMigrateRecordTypes(data, version, restricted);

    const operateOnTypes: DatabaseItemType[] = [];
    if (!isImportRavenFs) {
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

    const authenticationMethod = getEffectiveAuthenticationMethod(server.authenticationMethod, version);
    const isApiKey = authenticationMethod === "apiKey";
    const isWindows = authenticationMethod === "windows";
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
            ImportRavenFs: isImportRavenFs,
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
