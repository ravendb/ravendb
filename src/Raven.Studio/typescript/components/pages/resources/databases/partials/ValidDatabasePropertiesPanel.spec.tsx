import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { exportedForTesting } from "components/pages/resources/databases/partials/ValidDatabasePropertiesPanel";
import BackupInfo = Raven.Client.ServerWide.Operations.BackupInfo;
import { toDatabaseLocalInfo } from "components/common/shell/databasesSlice";
import { loadStatus, locationAwareLoadableData } from "components/models/common";
import { DatabaseLocalInfo } from "components/models/databases";
import moment from "moment";

const { findLatestBackup, getLocalGeneralInfo } = exportedForTesting;

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

    describe("getLocalGeneralInfo", () => {
        describe("non-sharded", () => {
            it("can get hasLocalNodeAllData", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo({ status: "idle", location: { nodeTag: "A" } }),
                    getDatabaseLocalInfo({ status: "success", location: { nodeTag: "B" } }),
                    getDatabaseLocalInfo({ status: "success", loadError: "Some Error", location: { nodeTag: "C" } }),
                ];

                expect(getLocalGeneralInfo(dbStates, "A").hasLocalNodeAllData).toBe(false);
                expect(getLocalGeneralInfo(dbStates, "B").hasLocalNodeAllData).toBe(true);
                expect(getLocalGeneralInfo(dbStates, "C").hasLocalNodeAllData).toBe(false);
            });

            it("can get total documents count", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo({ status: "success", location: { nodeTag: "A" }, documentsCount: 10 }),
                    getDatabaseLocalInfo({ status: "success", location: { nodeTag: "B" }, documentsCount: 20 }),
                ];

                expect(getLocalGeneralInfo(dbStates, "A").totalDocuments).toBe(10);
                expect(getLocalGeneralInfo(dbStates, "B").totalDocuments).toBe(20);
            });

            it("can get total size", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo({
                        location: { nodeTag: "A" },
                        totalSize: { SizeInBytes: 1, HumaneSize: "1 B" },
                        tempBuffersSize: { SizeInBytes: 2, HumaneSize: "2 B" },
                    }),
                    getDatabaseLocalInfo({
                        location: { nodeTag: "B" },
                        totalSize: { SizeInBytes: 3, HumaneSize: "3 B" },
                        tempBuffersSize: { SizeInBytes: 4, HumaneSize: "4 B" },
                    }),
                ];

                // totalSizeWithTempBuffers is totalSize + tempBuffersSize
                expect(getLocalGeneralInfo(dbStates, "A").totalSizeWithTempBuffers).toBe(3);
                expect(getLocalGeneralInfo(dbStates, "B").totalSizeWithTempBuffers).toEqual(7);
            });

            it("can get backup status", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo({
                        location: { nodeTag: "A" },
                        backupInfo: {
                            BackupTaskType: "Periodic",
                            Destinations: [],
                            IntervalUntilNextBackupInSec: 0,
                            LastBackup: null,
                        },
                    }),
                    getDatabaseLocalInfo({
                        location: { nodeTag: "B" },
                        backupInfo: {
                            BackupTaskType: "Periodic",
                            Destinations: [],
                            IntervalUntilNextBackupInSec: 0,
                            LastBackup: moment().subtract(1, "minute").format(),
                        },
                    }),
                ];

                expect(getLocalGeneralInfo(dbStates, "A").backupStatus).toEqual({
                    color: "danger",
                    text: "Never backed up",
                });
                expect(getLocalGeneralInfo(dbStates, "B").backupStatus).toEqual({
                    color: "success",
                    text: "Backed up a minute ago",
                });
            });
        });

        describe("sharded", () => {
            it("can get hasLocalNodeAllData", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo({ status: "success", location: { nodeTag: "A", shardNumber: 0 } }),
                    getDatabaseLocalInfo({ status: "success", location: { nodeTag: "A", shardNumber: 1 } }),
                    getDatabaseLocalInfo({
                        status: "success",
                        loadError: "Some error",
                        location: { nodeTag: "B", shardNumber: 0 },
                    }),
                    getDatabaseLocalInfo({ status: "success", location: { nodeTag: "B", shardNumber: 1 } }),
                ];

                expect(getLocalGeneralInfo(dbStates, "A").hasLocalNodeAllData).toBe(true);
                expect(getLocalGeneralInfo(dbStates, "B").hasLocalNodeAllData).toBe(false);
            });

            it("can get total documents count", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo({
                        status: "success",
                        location: { nodeTag: "A", shardNumber: 0 },
                        documentsCount: 10,
                    }),
                    getDatabaseLocalInfo({
                        status: "success",
                        location: { nodeTag: "A", shardNumber: 1 },
                        documentsCount: 20,
                    }),
                ];

                expect(getLocalGeneralInfo(dbStates, "A").totalDocuments).toBe(30);
            });

            it("can get total size", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo({
                        location: { nodeTag: "A", shardNumber: 0 },
                        totalSize: { SizeInBytes: 1, HumaneSize: "1 B" },
                        tempBuffersSize: { SizeInBytes: 2, HumaneSize: "2 B" },
                    }),
                    getDatabaseLocalInfo({
                        location: { nodeTag: "A", shardNumber: 1 },
                        totalSize: { SizeInBytes: 3, HumaneSize: "3 B" },
                        tempBuffersSize: { SizeInBytes: 4, HumaneSize: "4 B" },
                    }),
                ];

                // totalSizeWithTempBuffers is totalSize + tempBuffersSize

                const shard0TotalSize = 3;
                const shard1TotalSize = 7;
                const expectedTotalSize = shard0TotalSize + shard1TotalSize;

                expect(getLocalGeneralInfo(dbStates, "A").totalSizeWithTempBuffers).toBe(expectedTotalSize);
            });

            it("can get backup status", () => {
                const dbStates: locationAwareLoadableData<DatabaseLocalInfo>[] = [
                    getDatabaseLocalInfo({
                        location: { nodeTag: "A", shardNumber: 0 },
                        backupInfo: {
                            BackupTaskType: "Periodic",
                            Destinations: [],
                            IntervalUntilNextBackupInSec: 0,
                            LastBackup: null,
                        },
                    }),
                    getDatabaseLocalInfo({
                        location: { nodeTag: "A", shardNumber: 1 },
                        backupInfo: {
                            BackupTaskType: "Periodic",
                            Destinations: [],
                            IntervalUntilNextBackupInSec: 0,
                            LastBackup: moment().subtract(1, "minute").format(),
                        },
                    }),
                ];

                expect(getLocalGeneralInfo(dbStates, "A").backupStatus).toEqual({
                    color: "success",
                    text: "Backed up a minute ago",
                });
            });
        });

        function getDatabaseLocalInfo({
            status = "success",
            location,
            loadError,
            totalSize,
            tempBuffersSize,
            documentsCount,
            backupInfo,
        }: {
            location: databaseLocationSpecifier;
            status?: loadStatus;
            loadError?: string;
            documentsCount?: number;
            tempBuffersSize?: Raven.Client.Util.Size;
            totalSize?: Raven.Client.Util.Size;
            backupInfo?: BackupInfo;
        }): locationAwareLoadableData<DatabaseLocalInfo> {
            return {
                location,
                status,
                data: {
                    loadError,
                    databaseStatus: "Online",
                    indexingStatus: "Running",
                    documentsCount,
                    totalSize,
                    tempBuffersSize,
                    location,
                    indexingErrors: null,
                    alerts: null,
                    performanceHints: null,
                    name: "db",
                    backupInfo,
                },
            };
        }
    });
});
