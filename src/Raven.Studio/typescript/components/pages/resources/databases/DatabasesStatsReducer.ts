import { Reducer } from "react";
import DatabasesInfo = Raven.Client.ServerWide.Operations.DatabasesInfo;
import DatabaseInfo = Raven.Client.ServerWide.Operations.DatabaseInfo;
import { produce } from "immer";
import { DatabaseSharedInfo } from "../../../models/databases";


interface ActionStatsLoaded {
    stats: DatabasesInfo;
    type: "StatsLoaded";
}

type DatabasesStatsReducerAction =
    | ActionStatsLoaded;

interface DatabasesStatsState {
    databases: DatabaseSharedInfo[];
}

function mapToDatabaseShardedInfo(stats: DatabaseInfo): DatabaseSharedInfo {
    //TODO: 
    return {
        name: stats.Name,
        sharded: false
    }
}

export const databasesStatsReducer: Reducer<DatabasesStatsState, DatabasesStatsReducerAction> = (state: DatabasesStatsState, action: DatabasesStatsReducerAction): DatabasesStatsState => {
    switch (action.type) {
        case "StatsLoaded":
            return produce(state, draft => {
                const result: DatabaseSharedInfo[] = [];

                action.stats.Databases.forEach(incomingDb => {
                    const isSharded = incomingDb.Name.includes("$");
                    //TODO: this is temp impl!

                    if (isSharded) {
                        // take first shard for now
                        const [name, shard] = incomingDb.Name.split("$");
                        if (shard === "0") {
                            result.push({
                                name,
                                sharded: true
                            });
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
