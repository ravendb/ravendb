import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { findLatestBackup } from "components/pages/resources/databases/partials/ValidDatabasePropertiesPanel";
import { toDatabaseLocalInfo } from "components/common/shell/databasesSlice";
import BackupInfo = Raven.Client.ServerWide.Operations.BackupInfo;

describe("findLatestBackup", () => {
    it("get use latest backup", () => {
        const rawStates = [
            DatabasesStubs.studioDatabaseState("db1"),
            DatabasesStubs.studioDatabaseState("db1"),
            DatabasesStubs.studioDatabaseState("db1"),
        ];

        const localInfos = rawStates.map((x) => toDatabaseLocalInfo(x, "A"));

        localInfos[0].backupInfo = null;
        localInfos[1].backupInfo = stubBackupInfo("2022-04-03T12:12:13.6136291Z");
        localInfos[2].backupInfo = stubBackupInfo("2021-04-16T19:19:18.6136291Z");

        const backup = findLatestBackup(localInfos);
        expect(backup.LastBackup).toEqual("2022-04-03T12:12:13.6136291Z");
    });
});

function stubBackupInfo(date: string): BackupInfo {
    return {
        LastBackup: date,
        BackupTaskType: "Periodic",
        Destinations: [],
        IntervalUntilNextBackupInSec: 0,
    };
}
