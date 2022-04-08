import { Reducer } from "react";
import DatabasesInfo = Raven.Client.ServerWide.Operations.DatabasesInfo;
import DatabaseInfo = Raven.Client.ServerWide.Operations.DatabaseInfo;
import { produce } from "immer";
import { DatabaseSharedInfo } from "../../../models/databases";
import DatabaseUtils from "../../../utils/DatabaseUtils";
import { data } from "jquery";


interface ActionStatsLoaded {
    stats: DatabasesInfo;
    type: "StatsLoaded";
}

type DatabasesStatsReducerAction =
    | ActionStatsLoaded;

export interface DatabasesStatsState {
    databases: DatabaseSharedInfo[];
}

function mapToDatabaseShardedInfo(stats: DatabaseInfo): DatabaseSharedInfo {
    const sharded = DatabaseUtils.isSharded(stats.Name);
    return {
        name: DatabaseUtils.shardGroupKey(stats.Name),
        sharded,
        lockMode: stats.LockMode,
        encrypted: stats.IsEncrypted
    }
}

export const databasesStatsReducer: Reducer<DatabasesStatsState, DatabasesStatsReducerAction> = (state: DatabasesStatsState, action: DatabasesStatsReducerAction): DatabasesStatsState => {
    switch (action.type) {
        case "StatsLoaded":
            return produce(state, draft => {
                const result: DatabaseSharedInfo[] = [];

                action.stats.Databases.forEach(incomingDb => {
                    const isSharded = DatabaseUtils.isSharded(incomingDb.Name);
                    //TODO: this is temp impl!

                    if (isSharded) {
                        // take first shard for now
                        if (DatabaseUtils.shardNumber(incomingDb.Name) === 0) {
                            result.push(mapToDatabaseShardedInfo(incomingDb));
                        }
                    } else {
                        result.push(mapToDatabaseShardedInfo(incomingDb));
                    }
                });
                
                draft.databases = result;
            });
        default:
            console.warn("Unhandled action: ", action)
            return state;
    }
}


export const databasesStatsReducerInitializer = (): DatabasesStatsState => {
    return {
        databases: []
    }
}
