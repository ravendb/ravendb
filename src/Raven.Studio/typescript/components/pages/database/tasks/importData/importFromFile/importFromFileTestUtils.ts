import { connectionStringTokens, databaseSettingTokens, ongoingTaskTokens } from "./importFromFileUtils";

type ImportOptions = Raven.Client.Documents.Smuggler.DatabaseSmugglerImportOptions;
type DatabaseItemType = Raven.Client.Documents.Smuggler.DatabaseItemType;

export const adminDefaultOperateOnTypes: DatabaseItemType[] = [
    "DatabaseRecord",
    "Documents",
    "Conflicts",
    "Indexes",
    "RevisionDocuments",
    "Identities",
    "CompareExchange",
    "CounterGroups",
    "Attachments",
    "TimeSeries",
    "TimeSeriesDeletedRanges",
    "Subscriptions",
    "Tombstones",
    "CompareExchangeTombstones",
];

export const allSettingTokens = Object.values(databaseSettingTokens);
export const allOngoingTaskTokens = Object.values(ongoingTaskTokens);
export const allConnectionStringTokens = Object.values(connectionStringTokens);

export const adminDefaultDto: Partial<Record<keyof ImportOptions, unknown>> = {
    IncludeExpired: true,
    IncludeArtificial: false,
    IncludeArchived: true,
    TransformScript: "",
    RemoveAnalyzers: false,
    EncryptionKey: undefined,
    OperateOnTypes: adminDefaultOperateOnTypes.join(","),
    OperateOnDatabaseRecordTypes: "None",
    Collections: null,
    MaxReadOpsPerSecond: null,
};

export const without = <T>(list: T[], ...excluded: T[]) => list.filter((x) => !excluded.includes(x));

export const operateOnTypesOf = (dto: ImportOptions) => dto.OperateOnTypes.split(",");
export const recordTypesOf = (dto: ImportOptions) => dto.OperateOnDatabaseRecordTypes.split(",");
