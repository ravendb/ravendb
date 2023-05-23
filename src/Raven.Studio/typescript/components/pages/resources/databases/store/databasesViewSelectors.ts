import { RootState } from "components/store";
import { InputItem, locationAwareLoadableData } from "components/models/common";
import {
    DatabaseFilterByStateOption,
    DatabaseFilterCriteria,
    DatabaseLocalInfo,
    DatabaseSharedInfo,
    OrchestratorLocalInfo,
    TopLevelDatabaseInfo,
} from "components/models/databases";
import DatabaseUtils from "components/utils/DatabaseUtils";
import assertUnreachable from "components/utils/assertUnreachable";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { databasesViewSliceInternal } from "components/pages/resources/databases/store/databasesViewSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

const selectFilterByStateOptions = (store: RootState): InputItem<DatabaseFilterByStateOption>[] => {
    let error = 0,
        offline = 0,
        disabled = 0,
        online = 0,
        sharded = 0,
        nonSharded = 0,
        local = 0,
        remote = 0;

    databaseSelectors.allDatabases(store).forEach((db) => {
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
            case "Partially Online":
                online++;
                offline++;
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

    const localNodeTag = clusterSelectors.localNodeTag(store);

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

    const preMatchesStatus =
        databaseState === "Partially Online"
            ? filterStates.some((x) => x === "Online" || x === "Offline")
            : filterStates.includes(databaseState);

    const matchesStatus =
        !filterStates.some((x) => ["Online", "Offline", "Error", "Disabled"].includes(x)) ||
        preMatchesStatus ||
        databaseState === "Loading";

    if (!matchesStatus) {
        return false;
    }

    const matchesSharding =
        !filterStates.some((x) => ["Sharded", "NonSharded"].includes(x)) ||
        (filterStates.includes("Sharded") && db.sharded) ||
        (filterStates.includes("NonSharded") && !db.sharded);

    if (!matchesSharding) {
        return false;
    }

    return (
        !filterStates.some((x) => ["Local", "Remote"].includes(x)) ||
        (filterStates.includes("Local") && db.currentNode.relevant) ||
        (filterStates.includes("Remote") && !db.currentNode.relevant)
    );
};

const selectFilteredDatabaseNames =
    (criteria: DatabaseFilterCriteria) =>
    (store: RootState): string[] => {
        const allDatabases = databaseSelectors.allDatabases(store);

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

function selectOrchestratorState(name: string) {
    return (store: RootState) => {
        const db = databaseSelectors.databaseByName(name)(store);

        return db.nodes.map((nodeInfo): locationAwareLoadableData<OrchestratorLocalInfo> => {
            const nodeTag = nodeInfo.tag;
            const loadState = databasesViewSliceInternal.databaseDetailedLoadStatusSelectors.selectById(
                store.databasesView.detailedLoadStatus,
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
                    const data = databasesViewSliceInternal.orchestratorDetailedInfoSelectors.selectById(
                        store.databasesView.orchestratorDetailedInfo,
                        databasesViewSliceInternal.selectOrchestratorInfoId(name, nodeTag)
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

function getDatabaseLocalInfo(
    data: DatabaseLocalInfo,
    location: databaseLocationSpecifier
): locationAwareLoadableData<DatabaseLocalInfo> {
    if (!data) {
        return {
            location,
            status: "idle",
        };
    }

    if (data.loadError) {
        return {
            location,
            status: "success",
            data,
        };
    }

    switch (data.databaseStatus) {
        case "Online":
        case "None":
            return {
                location,
                status: "success",
                data,
            };
        case "Loading":
            return {
                location,
                status: "loading",
            };
        case "Error":
            return {
                location,
                status: "failure",
            };
        default:
            assertUnreachable(data.databaseStatus);
    }
}

export function selectDatabaseState(name: string) {
    return (store: RootState) => {
        const db = databaseSelectors.databaseByName(name)(store);

        const locations = DatabaseUtils.getLocations(db);

        return locations.map((location): locationAwareLoadableData<DatabaseLocalInfo> => {
            const loadState = databasesViewSliceInternal.databaseDetailedLoadStatusSelectors.selectById(
                store.databasesView.detailedLoadStatus,
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
                    const data = databasesViewSliceInternal.databaseDetailedInfoSelectors.selectById(
                        store.databasesView.databaseDetailedInfo,
                        databasesViewSliceInternal.selectDatabaseInfoId(name, location)
                    );

                    return getDatabaseLocalInfo(data, location);
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
        const db = databaseSelectors.databaseByName(name)(store);

        if (db.sharded) {
            const state = selectOrchestratorState(db.name)(store);

            return state.map((orchestratorState) => {
                const { data, location, ...rest } = orchestratorState;

                return {
                    data: data
                        ? databasesViewSliceInternal.orchestratorInfoIntoTopLevelDatabaseInfo(data, location.nodeTag)
                        : null,
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
                    ? databasesViewSliceInternal.databaseInfoIntoTopLevelDatabaseInfo(data, location.nodeTag)
                    : null,
                location,
                ...rest,
            };
        });
    };
}

export const databasesViewSelectors = {
    filterByStateOptions: selectFilterByStateOptions,
    filteredDatabaseNames: selectFilteredDatabaseNames,
};
