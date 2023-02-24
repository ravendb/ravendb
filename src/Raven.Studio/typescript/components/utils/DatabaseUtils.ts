import { DatabaseSharedInfo, ShardedDatabaseSharedInfo } from "components/models/databases";

export default class DatabaseUtils {
    static formatName(name: string) {
        if (!DatabaseUtils.isSharded(name)) {
            return name;
        }

        return DatabaseUtils.shardGroupKey(name) + " (shard #" + DatabaseUtils.shardNumber(name) + ")";
    }

    static isSharded(name: string) {
        return name.includes("$");
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

    static getLocations(db: DatabaseSharedInfo): databaseLocationSpecifier[] {
        if (db.sharded) {
            const shardedDb = db as ShardedDatabaseSharedInfo;

            const locations: databaseLocationSpecifier[] = shardedDb.shards.flatMap((shard) => {
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
}
