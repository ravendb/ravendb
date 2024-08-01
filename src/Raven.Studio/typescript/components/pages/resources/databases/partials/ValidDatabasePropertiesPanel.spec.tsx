import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { exportedForTesting } from "components/pages/resources/databases/partials/ValidDatabasePropertiesPanel";
import BackupInfo = Raven.Client.ServerWide.Operations.BackupInfo;
import { toDatabaseLocalInfo } from "components/common/shell/databasesSlice";
import { loadStatus, locationAwareLoadableData } from "components/models/common";
import { DatabaseLocalInfo } from "components/models/databases";

const { findLatestBackup, getIsGeneralInfoVisible } = exportedForTesting;

describe("ValidDatabasePropertiesPanel", () => {
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

        function stubBackupInfo(date: string): BackupInfo {
            return {
                LastBackup: date,
                BackupTaskType: "Periodic",
                Destinations: [],
                IntervalUntilNextBackupInSec: 0,
            };
        }
    });

    describe("getIsGeneralInfoVisible", () => {
        describe("non-sharded", () => {
            it("returns true if any of dbStates is successful without load error", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo("idle", null, { nodeTag: "A" }),
                    getDatabaseLocalInfo("success", null, { nodeTag: "B" }),
                    getDatabaseLocalInfo("idle", null, { nodeTag: "C" }),
                ];

                expect(getIsGeneralInfoVisible(dbStates, false)).toBe(true);
            });

            it("returns false if all dbStates are not successful", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo("loading", null, { nodeTag: "A" }),
                    getDatabaseLocalInfo("idle", null, { nodeTag: "B" }),
                    getDatabaseLocalInfo("failure", null, { nodeTag: "C" }),
                ];

                expect(getIsGeneralInfoVisible(dbStates, false)).toBe(false);
            });

            it("returns false if all dbStates have a load error", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo("success", "Some error 1", { nodeTag: "A" }),
                    getDatabaseLocalInfo("success", "Some error 2", { nodeTag: "B" }),
                    getDatabaseLocalInfo("success", "Some error 3", { nodeTag: "C" }),
                ];

                expect(getIsGeneralInfoVisible(dbStates, false)).toBe(false);
            });
        });

        describe("sharded", () => {
            it("returns true if any of dbStates for shard is successful without load error", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo("success", null, { nodeTag: "A", shardNumber: 0 }),
                    getDatabaseLocalInfo("success", null, { nodeTag: "B", shardNumber: 1 }),
                    getDatabaseLocalInfo("idle", null, { nodeTag: "C", shardNumber: 1 }),
                ];

                expect(getIsGeneralInfoVisible(dbStates, true)).toBe(true);
            });

            it("returns false if all dbStates for shard are not successful", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo("success", null, { nodeTag: "A", shardNumber: 0 }),
                    getDatabaseLocalInfo("idle", null, { nodeTag: "B", shardNumber: 1 }),
                    getDatabaseLocalInfo("failure", null, { nodeTag: "C", shardNumber: 1 }),
                ];

                expect(getIsGeneralInfoVisible(dbStates, true)).toBe(false);
            });

            it("returns false if all dbStates for shard have a load error", () => {
                const dbState: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo("success", null, { nodeTag: "A", shardNumber: 0 }),
                    getDatabaseLocalInfo("success", "Some error 1", { nodeTag: "B", shardNumber: 1 }),
                    getDatabaseLocalInfo("success", "Some error 2", { nodeTag: "C", shardNumber: 1 }),
                ];

                expect(getIsGeneralInfoVisible(dbState, true)).toBe(false);
            });
        });

        function getDatabaseLocalInfo(
            status: loadStatus,
            loadError: string | null,
            location: databaseLocationSpecifier
        ): locationAwareLoadableData<DatabaseLocalInfo> {
            return {
                location,
                status,
                data: {
                    loadError,
                    databaseStatus: "Online",
                    indexingStatus: "Running",
                    documentsCount: 10,
                    totalSize: null,
                    tempBuffersSize: null,
                    location,
                    indexingErrors: null,
                    alerts: null,
                    performanceHints: null,
                    name: "db",
                    backupInfo: null,
                },
            };
        }
    });
});
