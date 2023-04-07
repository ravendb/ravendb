import { RootState } from "components/store";
import DatabaseUtils from "components/utils/DatabaseUtils";
import {
    DatabaseFilterByStateOption,
    DatabaseLocalInfo,
    DatabaseSharedInfo,
    OrchestratorLocalInfo,
    ShardedDatabaseSharedInfo,
    TopLevelDatabaseInfo,
} from "components/models/databases";
import { InputItem, locationAwareLoadableData } from "components/models/common";
import { databasesSliceInternal } from "components/common/shell/databasesSlice";
import assertUnreachable from "components/utils/assertUnreachable";
import { selectLocalNodeTag } from "components/common/shell/clusterSlice";

export const selectActiveDatabase = (store: RootState) => store.databases.activeDatabase;

const {
    databasesSelectors,
    localDatabaseDetailedLoadStatusSelectors,
    localOrchestratorDetailedInfoSelectors,
    selectOrchestratorInfoId,
    localOrchestratorInfoIntoTopLevelDatabaseInfo,
    selectDatabaseInfoId,
    localDatabaseDetailedInfoSelectors,
} = databasesSliceInternal;

export const selectAllDatabases = (store: RootState) => databasesSelectors.selectAll(store.databases.databases);

export const selectAllDatabasesCount = (store: RootState) => databasesSelectors.selectTotal(store.databases.databases);

export const selectDatabaseSearchCriteria = (store: RootState) => store.databases.searchCriteria;

export const selectFilterByStateOptions = (store: RootState): InputItem<DatabaseFilterByStateOption>[] => {
    let error = 0,
        offline = 0,
        disabled = 0,
        online = 0,
        sharded = 0,
        nonSharded = 0,
        local = 0,
        remote = 0;

    selectAllDatabases(store).forEach((db) => {
        const perNodeState = selectDatabaseState(db.name)(store);
        const state = DatabaseUtils.getDatabaseState(db, perNodeState);

        switch (state) {
            case "Loading":
                break;
            case "Error":
                error++;
                break;
            case "Offline":
                offline++;
                break;
            case "Disabled":
                disabled++;
                break;
            case "Online":
                online++;
                break;
            default:
                assertUnreachable(state);
        }

        if (db.sharded) {
            sharded++;
        } else {
            nonSharded++;
        }

        if (db.currentNode.relevant) {
            local++;
        } else {
            remote++;
        }
    });

    const localNodeTag = selectLocalNodeTag(store);

    return [
        { value: "Online", label: "Online", count: online },
        { value: "Offline", label: "Offline", count: offline },
        { value: "Error", label: "Errored", count: error },
        { value: "Disabled", label: "Disabled", count: disabled },
        { value: "Sharded", label: "Sharded", count: sharded, verticalSeparatorLine: true },
        { value: "NonSharded", label: "Non Sharded", count: nonSharded },
        {
            value: "Local",
            label: `Local (Node ${localNodeTag})`,
            count: local,
            verticalSeparatorLine: true,
        },
        { value: "Remote", label: "Remote", count: remote },
    ];
};

const isDatabaseInFilterState = (
    store: RootState,
    db: DatabaseSharedInfo,
    filterStates: DatabaseFilterByStateOption[]
): boolean => {
    const perNodeState = selectDatabaseState(db.name)(store);
    const databaseState = DatabaseUtils.getDatabaseState(db, perNodeState);

    // prettier-ignore
    return (
        (
            !filterStates.some((x) => ["Online", "Offline", "Error", "Disabled"].includes(x)) ||
            filterStates.includes(databaseState) ||
            databaseState === "Loading"
        ) &&
        (
            !filterStates.some((x) => ["Sharded", "NonSharded"].includes(x)) ||
            (filterStates.includes("Sharded") && db.sharded) ||
            (filterStates.includes("NonSharded") && !db.sharded)
        ) &&
        (
            !filterStates.some((x) => ["Local", "Remote"].includes(x)) ||
            (filterStates.includes("Local") && db.currentNode.relevant) ||
            (filterStates.includes("Remote") && !db.currentNode.relevant)
        )
    );
};

export const selectFilteredDatabaseNames = (store: RootState): string[] => {
    const criteria = selectDatabaseSearchCriteria(store);
    const allDatabases = selectAllDatabases(store);

    if (!(criteria.name || criteria.states?.length > 0)) {
        return allDatabases.map((x) => x.name);
    }

    let filteredDatabases = allDatabases;

    if (criteria.name) {
        filteredDatabases = filteredDatabases.filter((db) =>
            db.name.toLowerCase().includes(criteria.name.toLowerCase())
        );
    }

    if (criteria.states?.length > 0) {
        filteredDatabases = filteredDatabases.filter((db) => isDatabaseInFilterState(store, db, criteria.states));
    }

    return filteredDatabases.map((x) => x.name);
};

export function selectDatabaseByName(name: string) {
    return (store: RootState) => {
        if (DatabaseUtils.isSharded(name)) {
            const rootDatabaseName = DatabaseUtils.shardGroupKey(name);
            const rootDatabase = databasesSelectors.selectById(
                store.databases.databases,
                rootDatabaseName
            ) as ShardedDatabaseSharedInfo;
            if (!rootDatabase) {
                return null;
            }
            return rootDatabase.shards.find((x) => x.name === name);
        }
        return databasesSelectors.selectById(store.databases.databases, name);
    };
}

function selectOrchestratorState(name: string) {
    return (store: RootState) => {
        const db = selectDatabaseByName(name)(store);

        return db.nodes.map((nodeInfo): locationAwareLoadableData<OrchestratorLocalInfo> => {
            const nodeTag = nodeInfo.tag;
            const loadState = localDatabaseDetailedLoadStatusSelectors.selectById(
                store.databases.localDetailedLoadStatus,
                nodeTag
            ) || {
                status: "idle",
                nodeTag,
            };

            switch (loadState.status) {
                case "idle":
                case "loading":
                case "failure":
                    return {
                        status: loadState.status,
                        location: {
                            nodeTag,
                        },
                        data: null,
                    };
                case "success": {
                    const data = localOrchestratorDetailedInfoSelectors.selectById(
                        store.databases.localOrchestratorDetailedInfo,
                        selectOrchestratorInfoId(name, nodeTag)
                    );

                    return {
                        location: {
                            nodeTag,
                        },
                        status: data ? "success" : "idle",
                        data,
                    };
                }
            }
        });
    };
}

export function selectDatabaseState(name: string) {
    return (store: RootState) => {
        const db = selectDatabaseByName(name)(store);

        const locations = DatabaseUtils.getLocations(db);

        return locations.map((location): locationAwareLoadableData<DatabaseLocalInfo> => {
            const loadState = localDatabaseDetailedLoadStatusSelectors.selectById(
                store.databases.localDetailedLoadStatus,
                location.nodeTag
            ) || {
                status: "idle",
                nodeTag: location.nodeTag,
            };

            switch (loadState.status) {
                case "idle":
                case "loading":
                case "failure":
                    return {
                        status: loadState.status,
                        location,
                    };
                case "success": {
                    const data = localDatabaseDetailedInfoSelectors.selectById(
                        store.databases.localDatabaseDetailedInfo,
                        selectDatabaseInfoId(name, location)
                    );

                    return {
                        location,
                        status: data ? "success" : "idle",
                        data,
                    };
                }
            }
        });
    };
}

/*
  For sharded databases it returns orchestrators state
  For non-sharded it return state from all nodes
 */
export function selectTopLevelState(name: string) {
    return (store: RootState): locationAwareLoadableData<TopLevelDatabaseInfo>[] => {
        const db = selectDatabaseByName(name)(store);

        if (db.sharded) {
            const state = selectOrchestratorState(db.name)(store);

            return state.map((orchestratorState) => {
                const { data, location, ...rest } = orchestratorState;

                return {
                    data: data ? localOrchestratorInfoIntoTopLevelDatabaseInfo(data, location.nodeTag) : null,
                    location,
                    ...rest,
                };
            });
        }

        const state = selectDatabaseState(db.name)(store);

        return state.map((dbState) => {
            const { data, location, ...rest } = dbState;

            return {
                data: data
                    ? databasesSliceInternal.localDatabaseInfoIntoTopLevelDatabaseInfo(data, location.nodeTag)
                    : null,
                location,
                ...rest,
            };
        });
    };
}
