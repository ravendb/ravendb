import { DatabaseLocalInfo, DatabaseSharedInfo, MergedDatabaseState } from "components/models/databases";
import BackupInfo = Raven.Client.ServerWide.Operations.BackupInfo;
import moment from "moment";
import { locationAwareLoadableData } from "components/models/common";

export default class DatabaseUtils {
    static formatName(name: string) {
        if (!DatabaseUtils.isSharded(name)) {
            return name;
        }

        return DatabaseUtils.shardGroupKey(name) + " (shard #" + DatabaseUtils.shardNumber(name) + ")";
    }

    static formatNameForFile(databaseName: string, location?: databaseLocationSpecifier) {
        if (!databaseName) {
            throw new Error("Must specify databaseName");
        }

        if (location) {
            return `${databaseName}_${location.nodeTag}${
                location?.shardNumber != null ? `_shard_${location.shardNumber}` : ""
            }`;
        }

        if (DatabaseUtils.isSharded(databaseName)) {
            return DatabaseUtils.shardGroupKey(databaseName) + "_shard_" + DatabaseUtils.shardNumber(databaseName);
        }

        return databaseName;
    }

    static isSharded(name: string) {
        return name?.includes("$");
    }

    static shardGroupKey(name: string) {
        return DatabaseUtils.isSharded(name) ? name.split("$")[0] : name;
    }

    static shardNumber(name: string): number {
        if (name.includes("$")) {
            return parseInt(name.split("$")[1], 10);
        } else {
            return undefined;
        }
    }

    static getDatabaseState(
        db: DatabaseSharedInfo,
        localInfo: locationAwareLoadableData<DatabaseLocalInfo>[]
    ): MergedDatabaseState {
        if (localInfo.every((x) => x.status === "loading" || x.status === "idle")) {
            return "Loading";
        }
        if (localInfo.some((x) => x.status === "success" && x.data.loadError)) {
            return "Error";
        }
        if (db.isDisabled) {
            return "Disabled";
        }

        const onlineCount = localInfo.filter((x) => x.status === "success" && x.data.upTime).length;
        if (onlineCount === localInfo.length) {
            return "Online";
        }

        if (onlineCount === 0) {
            return "Offline";
        }

        return "Partially Online";
    }

    static getLocations(db: DatabaseSharedInfo): databaseLocationSpecifier[] {
        if (db.isSharded) {
            const locations: databaseLocationSpecifier[] = db.shards.flatMap((shard) => {
                const shardNumber = DatabaseUtils.shardNumber(shard.name);

                return shard.nodes.map((node) => {
                    return {
                        nodeTag: node.tag,
                        shardNumber,
                    };
                });
            });

            return locations;
        } else {
            return db.nodes.map((node) => ({
                nodeTag: node.tag,
                shardNumber: undefined,
            }));
        }
    }

    static getFirstLocation(db: DatabaseSharedInfo, preferredNodeTag: string): databaseLocationSpecifier {
        const locations = DatabaseUtils.getLocations(db);
        const preferredMatch = locations.find((x) => x.nodeTag === preferredNodeTag);
        if (preferredMatch) {
            return preferredMatch;
        }

        return locations[0];
    }

    static computeBackupStatus(backupInfo: BackupInfo): { color: string; text: string } {
        if (!backupInfo || !backupInfo.LastBackup) {
            return {
                color: "danger",
                text: "Never backed up",
            };
        }

        const dateInUtc = moment.utc(backupInfo.LastBackup);
        const diff = moment().utc().diff(dateInUtc);
        const durationInSeconds = moment.duration(diff).asSeconds();

        const backupDate = moment.utc(backupInfo.LastBackup).local().fromNow();

        const text = `Backed up ${backupDate}`;

        return {
            text,
            color: durationInSeconds > dayAsSeconds ? "warning" : "success",
        };
    }

    static formatUptime(uptime: string): "Offline" | (string & NonNullable<unknown>) {
        return uptime ?? "Offline";
    }
}

const dayAsSeconds = 60 * 60 * 24;
