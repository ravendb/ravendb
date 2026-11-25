import { RestrictedKeys, getDefaultMigrateFormData, hasAnyMigrateInclude, toMigrateDto } from "./migrateRavenDbUtils";
import { SourceServerVersion, formatServerVersion, getUnsupportedDocumentToggles } from "./migrateVersionSupport";
import { MigrateRavenDbFormData } from "./migrateRavenDbValidation";
import {
    databaseSettingKeys,
    documentToggleKeys,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";

type MajorVersion = Raven.Server.Smuggler.Migration.MajorVersion;

function version(majorVersion: MajorVersion, buildVersion: number, fullVersion = ""): SourceServerVersion {
    return { majorVersion, buildVersion, productVersion: fullVersion, fullVersion };
}

const noRestrictions: RestrictedKeys = { settings: [], tasks: [], connectionStrings: [] };

function createFormData(): MigrateRavenDbFormData {
    const data = getDefaultMigrateFormData(true);
    data.server.serverUrl = "https://source:8080";
    data.server.databaseName = "Northwind";
    return data;
}

function createAllOffFormData(): MigrateRavenDbFormData {
    const data = createFormData();
    documentToggleKeys.forEach((key) => {
        data.documents[key] = false;
    });
    data.configuration.isIncludeIndexes = false;
    data.configuration.isIncludeIdentities = false;
    data.configuration.isIncludeConnectionStringsAndOngoingTasks = false;
    data.configuration.isImportAllSettings = false;
    databaseSettingKeys.forEach((key) => {
        data.configuration.databaseSettings[key] = false;
    });
    return data;
}

describe("formatServerVersion", () => {
    it("returns N/A for null", () => {
        expect(formatServerVersion(null)).toBe("N/A");
    });

    it("formats legacy versions with the build", () => {
        expect(formatServerVersion(version("V35", 35123))).toBe("3.5 (build: 35123)");
    });

    it("uses the full version for 4.x and above", () => {
        expect(formatServerVersion(version("V7", 70000, "7.1.3"))).toBe("7.1.3");
    });
});

describe("getUnsupportedDocumentToggles", () => {
    it("flags counters, time series and deleted ranges for 4.1", () => {
        expect(getUnsupportedDocumentToggles(version("V4", 41000))).toEqual([
            "isIncludeCounters",
            "isIncludeTimeSeries",
            "isIncludeTimeSeriesDeletedRanges",
        ]);
    });

    it("flags deleted ranges for 5.4", () => {
        expect(getUnsupportedDocumentToggles(version("V5", 54000))).toEqual(["isIncludeTimeSeriesDeletedRanges"]);
    });

    it("flags nothing for 7.1", () => {
        expect(getUnsupportedDocumentToggles(version("V7", 71000))).toEqual([]);
    });

    it("flags most toggles for 3.5", () => {
        expect(getUnsupportedDocumentToggles(version("V35", 35000)).sort()).toEqual(
            [
                "isIncludeAttachments",
                "isIncludeCounters",
                "isIncludeTimeSeries",
                "isIncludeTimeSeriesDeletedRanges",
                "isIncludeConflicts",
                "isIncludeCompareExchange",
                "isIncludeSubscriptions",
            ].sort()
        );
    });
});

describe("toMigrateDto", () => {
    it("skips toggles the source does not support", () => {
        const dto = toMigrateDto(createFormData(), version("V5", 54000), noRestrictions);
        const types = dto.MigrationSettings.OperateOnTypes.split(",");

        expect(types).not.toContain("TimeSeriesDeletedRanges");
        expect(types).toContain("TimeSeries");
        expect(types).toContain("CounterGroups");
        expect(types).not.toContain("LegacyAttachments");
        expect(types).not.toContain("Tombstones");
    });

    it("uses legacy attachments and no database record for a legacy source", () => {
        const data = createFormData();
        data.documents.isIncludeLegacyAttachments = true;

        const dto = toMigrateDto(data, version("V35", 35000), noRestrictions);
        const types = dto.MigrationSettings.OperateOnTypes.split(",");

        expect(types).toContain("LegacyAttachments");
        expect(types).not.toContain("DatabaseRecord");
        expect(types).not.toContain("Identities");
        expect(dto.MigrationSettings.OperateOnDatabaseRecordTypes).toBe("None");
    });

    it("ignores skip certificate validation for http", () => {
        const data = createFormData();
        data.server.serverUrl = "http://source:8080";
        data.server.isSkipServerCertificateValidation = true;

        expect(toMigrateDto(data, version("V7", 71000), noRestrictions).SkipServerCertificateValidation).toBe(false);
    });

    it("sends credentials only for the chosen method", () => {
        const data = createFormData();
        data.server.authenticationMethod = "apiKey";
        data.server.apiKey = "key";
        data.server.userName = "user";

        const dto = toMigrateDto(data, version("V7", 71000), noRestrictions);

        expect(dto.ApiKey).toBe("key");
        expect(dto.UserName).toBeNull();
    });

    it("sends None when RavenFS is imported", () => {
        const data = createFormData();
        data.server.isImportRavenFs = true;

        const dto = toMigrateDto(data, version("V35", 35000), noRestrictions);

        expect(dto.MigrationSettings.OperateOnTypes).toBe("None");
        expect(dto.MigrationSettings.ImportRavenFs).toBe(true);
    });
});

describe("toMigrateDto defaults", () => {
    it("includes legacy attachments for a default form and a legacy source", () => {
        const dto = toMigrateDto(createFormData(), version("V35", 35000), noRestrictions);

        expect(dto.MigrationSettings.OperateOnTypes.split(",")).toContain("LegacyAttachments");
    });

    it("skips counters and time series for a 4.1 source", () => {
        const dto = toMigrateDto(createFormData(), version("V4", 41000), noRestrictions);
        const types = dto.MigrationSettings.OperateOnTypes.split(",");

        expect(types).not.toContain("CounterGroups");
        expect(types).not.toContain("TimeSeries");
        expect(types).toContain("Documents");
    });

    it("sends windows credentials only", () => {
        const data = createFormData();
        data.server.authenticationMethod = "windows";
        data.server.userName = "user";
        data.server.password = "pass";
        data.server.domain = "corp";
        data.server.apiKey = "key";

        const dto = toMigrateDto(data, version("V7", 71000), noRestrictions);

        expect([dto.UserName, dto.Password, dto.Domain, dto.ApiKey]).toEqual(["user", "pass", "corp", null]);
    });
});

describe("hasAnyMigrateInclude", () => {
    it("is false when everything is off", () => {
        expect(hasAnyMigrateInclude(createAllOffFormData(), version("V7", 71000))).toBe(false);
    });

    it("is true when a supported toggle is on", () => {
        const data = createAllOffFormData();
        data.documents.isIncludeDocuments = true;

        expect(hasAnyMigrateInclude(data, version("V7", 71000))).toBe(true);
    });

    it("is true when only the default database record is on", () => {
        const data = getDefaultMigrateFormData(true);
        documentToggleKeys.forEach((key) => {
            data.documents[key] = false;
        });
        data.configuration.isIncludeIndexes = false;
        data.configuration.isIncludeIdentities = false;

        expect(hasAnyMigrateInclude(data, version("V7", 71000))).toBe(true);
    });

    it("ignores unsupported toggles", () => {
        const data = createAllOffFormData();
        data.documents.isIncludeTimeSeries = true;

        expect(hasAnyMigrateInclude(data, version("V4", 41000))).toBe(false);
    });
});
