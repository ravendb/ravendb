import {
    getDatabaseRecordTypes,
    getDefaultFormData,
    toImportDto,
    hasAnyInclude,
    getItemsToWarnAbout,
    buildImportCurlCommand,
} from "./importFromFileUtils";
import { ImportFromFileFormData, importFromFileSchema, databaseSettingKeys } from "./importFromFileValidation";

// Knockout defaults (importDatabaseModel + smugglerDatabaseRecord) with admin access — all
// admin-gated toggles on.
const createDefaultFormData = () => getDefaultFormData(true);

function setAllDatabaseSettings(data: ImportFromFileFormData, value: boolean) {
    databaseSettingKeys.forEach((key) => {
        data.configuration.databaseSettings[key] = value;
    });
}

function createAllOffFormData(): ImportFromFileFormData {
    const data = createDefaultFormData();
    const d = data.documents;
    d.isIncludeDocuments = false;
    d.isIncludeAttachments = false;
    d.isIncludeCounters = false;
    d.isIncludeRevisions = false;
    d.isIncludeTimeSeries = false;
    d.isIncludeTimeSeriesDeletedRanges = false;
    d.isIncludeArtificialDocuments = false;
    d.isIncludeArchivedDocuments = false;
    d.isIncludeExpiredDocuments = false;
    d.isIncludeConflicts = false;
    d.isIncludeCompareExchange = false;
    d.isIncludeLegacyAttachments = false;
    d.isIncludeDocumentsTombstones = false;
    d.isIncludeCompareExchangeTombstones = false;
    d.isIncludeSubscriptions = false;
    const c = data.configuration;
    c.isIncludeIndexes = false;
    c.isIncludeIndexHistory = false;
    c.isRemoveAnalyzers = false;
    c.isIncludeIdentities = false;
    c.isIncludeConnectionStringsAndOngoingTasks = false;
    c.isCustomizeOngoingTasks = false;
    c.isImportAllSettings = false;
    setAllDatabaseSettings(data, false);
    return data;
}

describe("importFromFileUtils", () => {
    describe("getDatabaseRecordTypes", () => {
        it("returns ['None'] in non-customized mode without index history", () => {
            expect(getDatabaseRecordTypes(createDefaultFormData())).toEqual(["None"]);
        });

        it("returns ['IndexesHistory'] in non-customized mode with index history", () => {
            const data = createDefaultFormData();
            data.configuration.isIncludeIndexHistory = true;
            expect(getDatabaseRecordTypes(data)).toEqual(["IndexesHistory"]);
        });

        it("emits explicit tokens without tasks when connection strings & ongoing tasks are excluded", () => {
            const data = createDefaultFormData();
            data.configuration.isIncludeConnectionStringsAndOngoingTasks = false;
            const types = getDatabaseRecordTypes(data);
            // must NOT collapse to ["None"] — the server expands "None" to ALL record types,
            // which would silently import the tasks the user excluded
            expect(types).not.toEqual(["None"]);
            expect(types).toContain("Settings");
            expect(types).not.toContain("RavenEtls");
            expect(types).not.toContain("RavenConnectionStrings");
            expect(types).not.toContain("PeriodicBackups");
        });

        it("emits only checked tasks and connection strings in customize-tasks mode", () => {
            const data = createDefaultFormData();
            data.configuration.isCustomizeOngoingTasks = true;
            Object.keys(data.configuration.ongoingTasks).forEach((key) => {
                data.configuration.ongoingTasks[key as keyof typeof data.configuration.ongoingTasks] = false;
            });
            Object.keys(data.configuration.connectionStrings).forEach((key) => {
                data.configuration.connectionStrings[key as keyof typeof data.configuration.connectionStrings] = false;
            });
            data.configuration.ongoingTasks.ravenEtls = true;
            const types = getDatabaseRecordTypes(data);
            expect(types).toContain("RavenEtls");
            expect(types).not.toContain("SqlEtls");
            expect(types).not.toContain("RavenConnectionStrings");
        });

        it("emits server-default-equivalent tokens minus restricted keys when only restrictions exist", () => {
            const types = getDatabaseRecordTypes(createDefaultFormData(), ["documentsCompression", "dataArchival"]);
            expect(types).not.toContain("DocumentsCompression");
            expect(types).not.toContain("DataArchival");
            expect(types).toContain("Settings");
            expect(types).toContain("RavenConnectionStrings");
            // parity with the server's expansion of "None" — tokens Studio has no toggle for
            expect(types).toContain("LockMode");
            expect(types).toContain("QueueSinks");
            expect(types).toContain("IndexesHistory");
        });

        it("excludes restricted keys even when explicitly checked in customize mode", () => {
            const data = createDefaultFormData();
            data.configuration.isImportAllSettings = false;
            setAllDatabaseSettings(data, false);
            data.configuration.databaseSettings.documentsCompression = true;
            data.configuration.databaseSettings.settings = true;
            data.configuration.isIncludeConnectionStringsAndOngoingTasks = false;
            data.configuration.isIncludeIndexHistory = false;
            const types = getDatabaseRecordTypes(data, ["documentsCompression"]);
            expect(types).not.toContain("DocumentsCompression");
            expect(types).toEqual(["Settings"]);
        });

        it("emits only checked settings in customize mode", () => {
            const data = createDefaultFormData();
            data.configuration.isImportAllSettings = false;
            setAllDatabaseSettings(data, false);
            data.configuration.databaseSettings.settings = true;
            data.configuration.isIncludeConnectionStringsAndOngoingTasks = false;
            data.configuration.isIncludeIndexHistory = false;
            expect(getDatabaseRecordTypes(data)).toEqual(["Settings"]);
        });
    });

    describe("toImportDto", () => {
        it("maps default form data like Knockout defaults", () => {
            const dto = toImportDto(createDefaultFormData());
            const types = (dto.OperateOnTypes as string).split(",");
            expect(types).toEqual(
                expect.arrayContaining(["DatabaseRecord", "Documents", "Conflicts", "Indexes", "RevisionDocuments",
                    "Identities", "CompareExchange", "CounterGroups", "Attachments", "TimeSeries",
                    "TimeSeriesDeletedRanges", "Subscriptions", "Tombstones", "CompareExchangeTombstones"])
            );
            expect(types).not.toContain("LegacyAttachments");
            expect(dto.OperateOnDatabaseRecordTypes).toBe("None");
            expect(dto.Collections).toBeNull();
            expect(dto.EncryptionKey).toBeUndefined();
            expect(dto.MaxReadOpsPerSecond).toBeNull();
            expect(dto.IncludeExpired).toBe(true);
            expect(dto.IncludeArtificial).toBe(false);
            expect(dto.IncludeArchived).toBe(true);
            expect(dto.RemoveAnalyzers).toBe(false);
            expect(dto.TransformScript).toBe("");
        });

        it("passes collections list when customize is on", () => {
            const data = createDefaultFormData();
            data.collections.isImportAllCollections = false;
            data.collections.includedCollections = ["Orders", "Employees"];
            expect(toImportDto(data).Collections).toEqual(["Orders", "Employees"]);
        });

        it("sets EncryptionKey and MaxReadOpsPerSecond only when enabled", () => {
            const data = createDefaultFormData();
            data.processing.isEncrypted = true;
            data.processing.encryptionKey = "key123";
            data.processing.isSetMaxReadOpsPerSecond = true;
            data.processing.maxReadOpsPerSecond = 500;
            const dto = toImportDto(data);
            expect(dto.EncryptionKey).toBe("key123");
            expect(dto.MaxReadOpsPerSecond).toBe(500);
        });
    });

    describe("hasAnyInclude", () => {
        it("returns false when every include and record type is off", () => {
            expect(hasAnyInclude(createAllOffFormData())).toBe(false);
        });

        it("returns true when database settings alone yield record types", () => {
            const data = createAllOffFormData();
            data.configuration.isImportAllSettings = true;
            expect(hasAnyInclude(data)).toBe(true);
        });
    });

    describe("getItemsToWarnAbout", () => {
        it("warns for counters/time series/revisions without documents", () => {
            const data = createDefaultFormData();
            data.documents.isIncludeDocuments = false;
            expect(getItemsToWarnAbout(data)).toEqual(["Counters", "Time Series", "Revisions"]);
        });

        it("returns empty when documents included", () => {
            expect(getItemsToWarnAbout(createDefaultFormData())).toEqual([]);
        });
    });

    describe("buildImportCurlCommand", () => {
        it("builds a PowerShell command with curl.exe, escaped quotes and windows file path", () => {
            const command = buildImportCurlCommand("PowerShell", createDefaultFormData(), "db1");
            expect(command).toContain("curl.exe");
            expect(command).toContain('\\"');
            expect(command).toContain("file=@.\\");
            expect(command).toContain("importOptions=");
            expect(command).toContain("/smuggler/import");
        });

        it("builds a Cmd command with curl.exe, escaped quotes and windows file path", () => {
            const command = buildImportCurlCommand("Cmd", createDefaultFormData(), "db1");
            expect(command).toContain("curl.exe");
            expect(command).toContain('\\"');
            expect(command).toContain("file=@.\\");
            expect(command).toContain("importOptions=");
            expect(command).toContain("/smuggler/import");
        });

        it("builds a Bash command with plain curl, unescaped json and unix file path", () => {
            const command = buildImportCurlCommand("Bash", createDefaultFormData(), "db1");
            expect(command).toContain("curl -F");
            expect(command).not.toContain("curl.exe");
            expect(command).toContain('"OperateOnTypes"');
            expect(command).not.toContain('\\"OperateOnTypes\\"');
            expect(command).toContain("file=@");
            expect(command).not.toContain("file=@.\\");
            expect(command).toContain("importOptions=");
            expect(command).toContain("/smuggler/import");
        });

        it("omits TransformScript from the json when the script is empty", () => {
            const command = buildImportCurlCommand("Bash", createDefaultFormData(), "db1");
            expect(command).not.toContain("TransformScript");
        });
    });

    describe("importFromFileSchema", () => {
        it("accepts a .ravendbdump file", async () => {
            await expect(
                importFromFileSchema.validateAt("file", { file: { name: "x.ravendbdump" } as File })
            ).resolves.toBeDefined();
        });

        it("rejects a RavenDB Snapshot file", async () => {
            await expect(
                importFromFileSchema.validateAt("file", { file: { name: "x.ravendb-snapshot" } as File })
            ).rejects.toThrow(/Snapshot/);
        });

        it("rejects a RavenDB Encrypted Snapshot file", async () => {
            await expect(
                importFromFileSchema.validateAt("file", { file: { name: "x.ravendb-encrypted-snapshot" } as File })
            ).rejects.toThrow(/Snapshot/);
        });
    });
});
