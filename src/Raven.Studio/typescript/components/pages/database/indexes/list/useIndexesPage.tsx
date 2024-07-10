import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import { BulkIndexOperationConfirm } from "components/pages/database/indexes/list/BulkIndexOperationConfirm";
import {
    IndexFilterCriteria,
    IndexGroup,
    IndexNodeInfoDetails,
    IndexSharedInfo,
    IndexStatus,
    IndexType,
} from "components/models/indexes";
import {
    indexesStatsReducer,
    indexesStatsReducerInitializer,
} from "components/pages/database/indexes/list/IndexesStatsReducer";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import useInterval from "hooks/useInterval";
import messagePublisher from "common/messagePublisher";
import IndexUtils from "components/utils/IndexUtils";
import genUtils from "common/generalUtils";
import { delay } from "components/utils/common";
import { useServices } from "hooks/useServices";
import { useEventsCollector } from "hooks/useEventsCollector";
import { useChanges } from "hooks/useChanges";
import { shardingTodo } from "common/developmentHelper";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import collection from "models/database/documents/collection";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import { useAsync } from "react-async-hook";
import { InputItem, InputItemLimit } from "components/models/common";
import { DatabaseActionContexts } from "components/common/MultipleDatabaseLocationSelector";
import ActionContextUtils from "components/utils/actionContextUtils";
import { getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { throttledUpdateLicenseLimitsUsage } from "components/common/shell/setup";
import useConfirm from "components/common/ConfirmDialog";
import React from "react";
import { Alert } from "reactstrap";
import DeleteIndexesConfirmBody, { DeleteIndexesConfirmBodyProps } from "../shared/DeleteIndexesConfirmBody";
import assertUnreachable from "components/utils/assertUnreachable";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

type IndexEvent =
    | Raven.Client.Documents.Changes.IndexChange
    | Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged;

export interface ResetIndexesData {
    confirmData: {
        indexNames: string[];
        mode?: Raven.Client.Documents.Indexes.IndexResetMode;
    };
    onConfirm: (contexts: DatabaseActionContexts[]) => Promise<void>;
    openConfirm: (indexNames: string[], mode?: Raven.Client.Documents.Indexes.IndexResetMode) => void;
    closeConfirm: () => void;
}

export interface SwapSideBySideData {
    indexName: string;
    setIndexName: React.Dispatch<React.SetStateAction<string>>;
    onConfirm: (contexts: DatabaseActionContexts[]) => Promise<void>;
    inProgress: (indexName: string) => boolean;
}

export function useIndexesPage(stale: boolean) {
    const autoDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfAutoIndexesPerDatabase"));
    const staticDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfStaticIndexesPerDatabase"));

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const locations = useMemo(() => DatabaseUtils.getLocations(db), [db]);

    const [bulkOperationConfirm, setBulkOperationConfirm] = useState<{
        type: Parameters<typeof BulkIndexOperationConfirm>[0]["type"];
        indexes: IndexSharedInfo[];
        allActionContexts: DatabaseActionContexts[];
        onConfirm: (contextPoints: DatabaseActionContexts[]) => void;
    }>();

    const { indexesService } = useServices();
    const eventsCollector = useEventsCollector();
    const { databaseChangesApi, serverNotifications } = useChanges();

    const [resetIndexesConfirmData, setResetIndexesConfirmData] = useState<ResetIndexesData["confirmData"]>(null);

    const openResetIndexConfirm = (indexNames: string[], mode?: Raven.Client.Documents.Indexes.IndexResetMode) => {
        setResetIndexesConfirmData({
            indexNames,
            mode,
        });
    };

    const closeResetIndexConfirm = () => {
        setResetIndexesConfirmData(null);
    };

    const confirm = useConfirm();

    const [stats, dispatch] = useReducer(indexesStatsReducer, locations, indexesStatsReducerInitializer);

    const [filter, setFilter] = useState<IndexFilterCriteria>(() => {
        if (stale) {
            return {
                ...defaultFilterCriteria,
                statuses: ["Stale"],
            };
        } else {
            return defaultFilterCriteria;
        }
    });

    shardingTodo("ANY");
    const globalIndexingStatus: IndexRunningStatus = "Running"; //TODO:

    const [selectedIndexes, setSelectedIndexes] = useState<string[]>([]);

    const [swapSideBySideConfirmIndexName, setSwapSideBySideConfirmIndexName] = useState<string>(null);
    const [swapNowProgress, setSwapNowProgress] = useState<string[]>([]);

    //TODO:
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [globalLockChanges, setGlobalLockChanges] = useState(false);

    const { regularIndexes, groups, replacements } = useMemo(() => {
        const collections = collectionsTracker.default.collections();
        const groupedIndexes = groupAndFilterIndexStats(stats.indexes, collections, filter, globalIndexingStatus);

        return groupedIndexes;
    }, [filter, stats.indexes]);

    const fetchProgress = async (location: databaseLocationSpecifier) => {
        try {
            const progress = await indexesService.getProgress(db.name, location);

            dispatch({
                type: "ProgressLoaded",
                progress,
                location,
            });
        } catch (e) {
            dispatch({
                type: "ProgressLoadError",
                error: e,
                location,
            });
        }
    };

    const fetchStats = useCallback(
        async (location: databaseLocationSpecifier) => {
            const stats = await indexesService.getStats(db.name, location);
            dispatch({
                type: "StatsLoaded",
                location,
                stats,
            });
        },
        [db.name, indexesService]
    );

    const throttledRefresh = useRef(
        _.throttle(() => {
            locations.forEach(fetchStats);
        }, 3_000)
    );

    const throttledProgressRefresh = useRef(
        _.throttle(() => {
            locations.forEach((location) => fetchProgress(location));
        }, 10_000)
    );

    useInterval(() => {
        if (stats.indexes.length === 0) {
            return;
        }

        const anyStale = stats.indexes.some((x) => x.nodesInfo.some((n) => n.details && n.details.stale));

        if (anyStale) {
            throttledProgressRefresh.current();
        }
    }, 3_000);

    const [resettingIndex, setResettingIndex] = useState(false);

    const { loading } = useAsync(async () => {
        const tasks = locations.map(fetchStats);
        try {
            await Promise.race(tasks);
            throttledUpdateLicenseLimitsUsage();
        } catch {
            // ignore - we handle that below
        }

        Promise.all(tasks).finally(() => throttledProgressRefresh.current());
    }, []);

    const currentProcessor = useRef<(e: IndexEvent) => void>();

    useEffect(() => {
        currentProcessor.current = (e: IndexEvent) => {
            if (e.Type === "IndexAdded" || e.Type === "IndexRemoved") {
                throttledUpdateLicenseLimitsUsage();
            }

            if (!filter.autoRefresh || resettingIndex) {
                return;
            }

            if (e.Type === "BatchCompleted") {
                throttledProgressRefresh.current();
                throttledRefresh.current();
            }

            throttledRefresh.current();
        };
    }, [filter.autoRefresh, resettingIndex]);

    const handleDatabaseChanges = useCallback(
        (e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => {
            if (e.ChangeType !== "RemoveNode" && e.ChangeType !== "Update") {
                return;
            }

            dispatch({
                type: "LocationsLoaded",
                locations: locations,
            });

            currentProcessor.current(e);
        },
        [locations]
    );

    useEffect(() => {
        if (databaseChangesApi && serverNotifications) {
            const watchAllIndexes = databaseChangesApi.watchAllIndexes((e) => currentProcessor.current(e));
            const watchAllDatabaseChanges = serverNotifications.watchAllDatabaseChanges((e) =>
                handleDatabaseChanges(e)
            );

            return () => {
                watchAllIndexes.off();
                watchAllDatabaseChanges.off();
            };
        }
    }, [databaseChangesApi, handleDatabaseChanges, serverNotifications]);

    const highlightUsed = useRef<boolean>(false);

    const highlightCallback = useCallback((node: HTMLElement) => {
        if (node && !highlightUsed.current) {
            node.scrollIntoView({ behavior: "smooth", block: "center" });
            highlightUsed.current = true;

            setTimeout(() => {
                if (document.body.contains(node)) {
                    node.classList.add("blink-style-basic");
                }
            }, 600);
        }
    }, []);

    const getSelectedIndexes = useCallback(
        (): IndexSharedInfo[] => stats.indexes.filter((x) => selectedIndexes.includes(x.name)),
        [selectedIndexes, stats]
    );

    const onDisableIndexesConfirm = useCallback(
        async (indexes: IndexSharedInfo[], contexts: DatabaseActionContexts[]) => {
            eventsCollector.reportEvent("index", "toggle-status");
            const disableRequests: Promise<void>[] = [];

            for (const index of indexes) {
                for (const { nodeTag, shardNumbers } of contexts) {
                    const locations = ActionContextUtils.getLocations(nodeTag, shardNumbers);

                    for (const location of locations) {
                        const indexStatus = index.nodesInfo.find((x) => _.isEqual(x.location, location))?.details
                            ?.status;

                        if (indexStatus === "Disabled") {
                            continue;
                        }

                        disableRequests.push(
                            indexesService.disable(index, db.name, location).then(() => {
                                dispatch({
                                    type: "DisableIndexing",
                                    indexName: index.name,
                                    location,
                                });
                            })
                        );
                    }
                }
            }

            await Promise.all(disableRequests);
            messagePublisher.reportSuccess(`${indexes.length === 1 ? "Index" : "Indexes"} disabled successfully.`);
        },
        [indexesService, db.name, eventsCollector]
    );

    const disableIndexes = useCallback(
        async (indexes: IndexSharedInfo[]) => {
            setBulkOperationConfirm({
                type: "disable",
                indexes,
                allActionContexts: ActionContextUtils.getContexts(locations),
                onConfirm: (contexts: DatabaseActionContexts[]) => onDisableIndexesConfirm(indexes, contexts),
            });
        },
        [locations, onDisableIndexesConfirm]
    );

    const onPauseIndexesConfirm = useCallback(
        async (indexes: IndexSharedInfo[], contexts: DatabaseActionContexts[]) => {
            eventsCollector.reportEvent("index", "toggle-status");
            const pauseRequests: Promise<void>[] = [];

            for (const index of indexes) {
                for (const { nodeTag, shardNumbers } of contexts) {
                    const locations = ActionContextUtils.getLocations(nodeTag, shardNumbers);

                    for (const location of locations) {
                        const indexStatus = index.nodesInfo.find((x) => _.isEqual(x.location, location))?.details
                            ?.status;

                        if (indexStatus === "Paused" || indexStatus === "Disabled") {
                            continue;
                        }

                        pauseRequests.push(
                            indexesService.pause(index, db.name, location).then(() => {
                                dispatch({
                                    type: "PauseIndexing",
                                    indexName: index.name,
                                    location,
                                });
                            })
                        );
                    }
                }
            }

            await Promise.all(pauseRequests);
            messagePublisher.reportSuccess(`${indexes.length === 1 ? "Index" : "Indexes"} paused successfully.`);
        },
        [eventsCollector, indexesService, db.name]
    );

    const pauseIndexes = useCallback(
        async (indexes: IndexSharedInfo[]) => {
            setBulkOperationConfirm({
                type: "pause",
                indexes,
                allActionContexts: ActionContextUtils.getContexts(locations),
                onConfirm: (contexts: DatabaseActionContexts[]) => onPauseIndexesConfirm(indexes, contexts),
            });
        },
        [locations, onPauseIndexesConfirm]
    );

    const onStartIndexesConfirm = useCallback(
        async (indexes: IndexSharedInfo[], contexts: DatabaseActionContexts[]) => {
            eventsCollector.reportEvent("index", "toggle-status");
            const startRequests: Promise<void>[] = [];

            for (const index of indexes) {
                for (const { nodeTag, shardNumbers } of contexts) {
                    const locations = ActionContextUtils.getLocations(nodeTag, shardNumbers);

                    for (const location of locations) {
                        const details = index.nodesInfo.find((x) => _.isEqual(x.location, location))?.details;

                        if (details?.status === "Paused" && details?.state !== "Error") {
                            startRequests.push(
                                indexesService.resume(index, db.name, location).then(() => {
                                    dispatch({
                                        type: "ResumeIndexing",
                                        indexName: index.name,
                                        location,
                                    });
                                })
                            );
                        } else {
                            startRequests.push(
                                indexesService.enable(index, db.name, location).then(() => {
                                    dispatch({
                                        type: "EnableIndexing",
                                        indexName: index.name,
                                        location,
                                    });
                                })
                            );
                        }
                    }
                }
            }

            await Promise.all(startRequests);
            messagePublisher.reportSuccess(`${indexes.length === 1 ? "Index" : "Indexes"} started successfully.`);
        },
        [eventsCollector, indexesService, db.name]
    );

    const startIndexes = useCallback(
        async (indexes: IndexSharedInfo[]) => {
            setBulkOperationConfirm({
                type: "start",
                indexes,
                allActionContexts: ActionContextUtils.getContexts(locations),
                onConfirm: (contexts: DatabaseActionContexts[]) => onStartIndexesConfirm(indexes, contexts),
            });
        },
        [locations, onStartIndexesConfirm]
    );

    const setIndexLockModeInternal = useCallback(
        async (index: IndexSharedInfo, lockMode: IndexLockMode) => {
            await indexesService.setLockMode([index], lockMode, db.name);

            dispatch({
                type: "SetLockMode",
                lockMode,
                indexName: index.name,
            });
        },
        [db.name, indexesService]
    );

    const setLockModeSelectedIndexes = useCallback(
        async (lockMode: IndexLockMode, indexes: IndexSharedInfo[]) => {
            eventsCollector.reportEvent("index", "set-lock-mode-selected", lockMode);

            if (indexes.length) {
                setGlobalLockChanges(true);

                try {
                    while (indexes.length) {
                        await setIndexLockModeInternal(indexes.pop(), lockMode);
                    }
                    messagePublisher.reportSuccess("Lock mode was set to: " + IndexUtils.formatLockMode(lockMode));
                } finally {
                    setGlobalLockChanges(false);
                }
            }
        },
        [eventsCollector, setIndexLockModeInternal]
    );

    const confirmSetLockModeSelectedIndexes = useCallback(
        async (lockMode: IndexLockMode) => {
            const indexes = getSelectedIndexes().filter(
                (index) => index.type !== "AutoMap" && index.type !== "AutoMapReduce"
            );

            const isConfirmed = await confirm({
                icon: IndexUtils.getLockIcon(lockMode),
                title: (
                    <span>
                        Do you want to <strong>{IndexUtils.formatLockMode(lockMode)}</strong> selected indexes?
                    </span>
                ),
                actionColor: "primary",
                message: (
                    <div>
                        <ul className="overflow-auto" style={{ maxHeight: "200px" }}>
                            {indexes.map((x) => (
                                <li key={x.name}>{x.name}</li>
                            ))}
                        </ul>
                        <Alert color="info">
                            Static-indexes only will be set, &apos;Lock Mode&apos; is not relevant for auto-indexes.
                        </Alert>
                    </div>
                ),
            });

            if (isConfirmed) {
                await setLockModeSelectedIndexes(lockMode, indexes);
            }
        },
        [getSelectedIndexes, confirm, setLockModeSelectedIndexes]
    );

    const confirmDeleteIndexes = async (indexes: IndexSharedInfo[]): Promise<void> => {
        eventsCollector.reportEvent("indexes", "delete");

        const confirmData = getIndexInfoForDelete(indexes);

        const isConfirmed = await confirm({
            title: `Delete ${confirmData.indexesInfoForDelete.length === 1 ? "index" : "indexes"}?`,
            message: <DeleteIndexesConfirmBody {...confirmData} />,
            icon: "trash",
            confirmText: "Delete",
            actionColor: "danger",
        });

        if (!isConfirmed) {
            return;
        }

        const tasks = confirmData.indexesInfoForDelete.map((x) => indexesService.deleteIndex(x.indexName, db.name));
        await Promise.all(tasks);

        if (tasks.length > 1) {
            messagePublisher.reportSuccess(`Successfully deleted ${tasks.length} indexes!`);
        }

        const deletedIndexNames = confirmData.indexesInfoForDelete.map((x) => x.indexName);

        setSelectedIndexes((x) => x.filter((x) => !deletedIndexNames.includes(x)));
        dispatch({
            type: "DeleteIndexes",
            indexNames: deletedIndexNames,
        });
    };

    const setIndexPriority = async (index: IndexSharedInfo, priority: IndexPriority) => {
        await indexesService.setPriority(index, priority, db.name);

        dispatch({
            type: "SetPriority",
            priority,
            indexName: index.name,
        });
    };

    const setIndexLockMode = useCallback(
        async (index: IndexSharedInfo, lockMode: IndexLockMode) => {
            await setIndexLockModeInternal(index, lockMode);
            messagePublisher.reportSuccess("Lock mode was set to: " + IndexUtils.formatLockMode(lockMode));
        },
        [setIndexLockModeInternal]
    );

    const toggleSelection = (index: IndexSharedInfo) => {
        setSelectedIndexes((s) => {
            if (s.includes(index.name)) {
                return s.filter((x) => x !== index.name);
            } else {
                return s.concat(index.name);
            }
        });
    };

    const openFaulty = async (index: IndexSharedInfo, location: databaseLocationSpecifier) => {
        const isConfirmed = await confirm({
            title: (
                <span>
                    Do you want to open faulty index <strong>{index.name}</strong>?
                </span>
            ),
            confirmText: "Open",
            actionColor: "primary",
        });

        if (isConfirmed) {
            eventsCollector.reportEvent("indexes", "open");
            await indexesService.openFaulty(index, db.name, location);
        }
    };

    const onResetIndexConfirm = async (contexts: DatabaseActionContexts[]) => {
        eventsCollector.reportEvent("indexes", "reset");
        const resetRequests: Promise<void>[] = [];
        setResettingIndex(true);

        for (const indexName of resetIndexesConfirmData.indexNames) {
            try {
                for (const { nodeTag, shardNumbers } of contexts) {
                    const locations = ActionContextUtils.getLocations(nodeTag, shardNumbers);

                    for (const location of locations) {
                        resetRequests.push(
                            indexesService
                                .resetIndex(indexName, db.name, resetIndexesConfirmData.mode, location)
                                .then(() => {
                                    dispatch({
                                        type: "ResetIndex",
                                        indexName: indexName,
                                        location,
                                    });
                                })
                        );
                    }
                }

                await Promise.all(resetRequests);
                messagePublisher.reportSuccess("Index " + indexName + " restarted successfully.");
            } finally {
                // wait a bit and trigger refresh
                await delay(1_000);

                throttledRefresh.current();
                setResettingIndex(false);
            }
        }
    };

    const onSwapSideBySideIndexConfirm = async (contexts: DatabaseActionContexts[]) => {
        eventsCollector.reportEvent("index", "swap-side-by-side");
        const swapRequests: Promise<void>[] = [];
        setSwapNowProgress((x) => [...x, swapSideBySideConfirmIndexName]);

        try {
            for (const { nodeTag, shardNumbers } of contexts) {
                const locations = ActionContextUtils.getLocations(nodeTag, shardNumbers);

                for (const location of locations) {
                    swapRequests.push(indexesService.forceReplace(swapSideBySideConfirmIndexName, db.name, location));
                }
            }

            await Promise.all(swapRequests);
            messagePublisher.reportSuccess("Index " + swapSideBySideConfirmIndexName + " replaced successfully.");
        } finally {
            setSwapNowProgress((item) => item.filter((x) => x !== swapSideBySideConfirmIndexName));
        }
    };

    const toggleSelectAll = () => {
        eventsCollector.reportEvent("indexes", "toggle-select-all");

        const allVisibleIndexes = getAllIndexes(groups, replacements).map((x) => x.name);
        const selectionState = genUtils.getSelectionState(allVisibleIndexes, selectedIndexes);

        if (selectionState === "Empty") {
            setSelectedIndexes((prevState) => [...prevState, ...allVisibleIndexes]);
        } else {
            setSelectedIndexes((prevState) => prevState.filter((x) => !allVisibleIndexes.includes(x)));
        }
    };

    const filterByTypeOptions: InputItem<IndexType>[] = useMemo(() => {
        const autoIndexCount = stats.indexes.filter((x) => IndexUtils.isAutoIndex(x)).length;
        const staticIndexCount = stats.indexes.length - autoIndexCount;

        const autoIndexReachStatus = getLicenseLimitReachStatus(autoIndexCount, autoDatabaseLimit);
        const staticIndexReachStatus = getLicenseLimitReachStatus(staticIndexCount, staticDatabaseLimit);

        const staticIndexInputLimit: InputItemLimit =
            staticIndexReachStatus !== "notReached"
                ? {
                      value: staticDatabaseLimit,
                      badgeColor: staticIndexReachStatus === "closeToLimit" ? "warning" : "danger",
                      message: `Your license allows ${staticDatabaseLimit} Static Indexes`,
                  }
                : null;

        const autoIndexInputLimit: InputItemLimit =
            autoIndexReachStatus !== "notReached"
                ? {
                      value: autoDatabaseLimit,
                      badgeColor: autoIndexReachStatus === "closeToLimit" ? "warning" : "danger",
                      message: `Your license allows ${autoDatabaseLimit} Auto Indexes`,
                  }
                : null;

        return [
            {
                value: "StaticIndex",
                label: "Static",
                count: staticIndexCount,
                limit: staticIndexInputLimit,
            },
            {
                value: "AutoIndex",
                label: "Auto",
                count: autoIndexCount,
                limit: autoIndexInputLimit,
            },
        ] satisfies InputItem<IndexType>[];
    }, [autoDatabaseLimit, staticDatabaseLimit, stats.indexes]);

    const filterByStatusOptions: InputItem<IndexStatus>[] = useMemo(() => {
        let normal = 0,
            errorOrFaulty = 0,
            stale = 0,
            paused = 0,
            disabled = 0,
            idle = 0;

        for (const index of stats.indexes) {
            if (anyMatch(index, (x) => IndexUtils.isNormalState(x, globalIndexingStatus))) {
                normal++;
            }
            if (anyMatch(index, IndexUtils.isErrorState) || IndexUtils.hasAnyFaultyNode(index)) {
                errorOrFaulty++;
            }
            if (anyMatch(index, (x) => x.stale)) {
                stale++;
            }
            if (anyMatch(index, (x) => IndexUtils.isPausedState(x, globalIndexingStatus))) {
                paused++;
            }
            if (anyMatch(index, (x) => IndexUtils.isDisabledState(x, globalIndexingStatus))) {
                disabled++;
            }
            if (anyMatch(index, (x) => IndexUtils.isIdleState(x, globalIndexingStatus))) {
                idle++;
            }
            // TODO: add "RollingDeployment"
        }

        return [
            { value: "Normal", label: "Normal", count: normal },
            { value: "ErrorOrFaulty", label: "Error Or Faulty", count: errorOrFaulty },
            { value: "Stale", label: "Stale", count: stale },
            { value: "Paused", label: "Paused", count: paused },
            { value: "Disabled", label: "Disabled", count: disabled },
            { value: "Idle", label: "Idle", count: idle },
        ];
    }, [stats.indexes]);

    return {
        loading,
        bulkOperationConfirm,
        setBulkOperationConfirm,
        stats,
        selectedIndexes,
        toggleSelectAll,
        onSelectCancel: () => setSelectedIndexes([]),
        filter,
        setFilter,
        filterByStatusOptions,
        filterByTypeOptions,
        regularIndexes,
        groups,
        replacements,
        swapNowProgress,
        highlightCallback,
        confirmSetLockModeSelectedIndexes,
        allIndexesCount: stats.indexes.length,
        setIndexPriority,
        getSelectedIndexes,
        startIndexes,
        disableIndexes,
        pauseIndexes,
        setIndexLockMode,
        toggleSelection,
        resetIndexData: {
            confirmData: resetIndexesConfirmData,
            onConfirm: onResetIndexConfirm,
            closeConfirm: closeResetIndexConfirm,
            openConfirm: openResetIndexConfirm,
        } satisfies ResetIndexesData,
        swapSideBySideData: {
            indexName: swapSideBySideConfirmIndexName,
            setIndexName: setSwapSideBySideConfirmIndexName,
            onConfirm: onSwapSideBySideIndexConfirm,
            inProgress: (indexName: string) => swapNowProgress.includes(indexName),
        } satisfies SwapSideBySideData,
        openFaulty,
        confirmDeleteIndexes,
        globalIndexingStatus,
    };
}

export const defaultFilterCriteria: IndexFilterCriteria = {
    statuses: [],
    types: [],
    autoRefresh: true,
    showOnlyIndexesWithIndexingErrors: false,
    searchText: "",
    sortBy: "name",
    sortDirection: "asc",
    groupBy: "Collection",
};

export function getAllIndexes(groups: IndexGroup[], replacements: IndexSharedInfo[]) {
    const allIndexes: IndexSharedInfo[] = [];

    for (const index of groups.map((x) => x.indexes).flat()) {
        allIndexes.push(index);

        const replacement = replacements.find((x) => x.name === IndexUtils.SideBySideIndexPrefix + index.name);

        if (replacement) {
            allIndexes.push(replacement);
        }
    }

    return allIndexes;
}

function getSortedRegularIndexes(
    indexes: IndexSharedInfo[],
    { sortBy, sortDirection }: IndexFilterCriteria
): IndexSharedInfo[] {
    switch (sortBy) {
        case "name":
            return _.orderBy(indexes, [sortBy], [sortDirection]);
        case "createdTimestamp":
            return _.orderBy(indexes, (index: IndexSharedInfo) => IndexUtils.getEarliestCreatedTimestamp(index), [
                sortDirection,
            ]);
        case "lastIndexingTime":
        case "lastQueryingTime":
            return _.orderBy(
                indexes,
                (index: IndexSharedInfo) => _.max(index.nodesInfo.map((nodesInfo) => nodesInfo.details?.[sortBy])),
                [sortDirection]
            );
        default:
            assertUnreachable(sortBy);
    }
}

function groupAndFilterIndexStats(
    indexes: IndexSharedInfo[],
    collections: collection[],
    filter: IndexFilterCriteria,
    globalIndexingStatus: IndexRunningStatus
): { regularIndexes: IndexSharedInfo[]; groups: IndexGroup[]; replacements: IndexSharedInfo[] } {
    const result = new Map<string, IndexGroup>();

    const replacements = indexes.filter(IndexUtils.isSideBySide);
    const regularIndexes = indexes.filter((x) => !IndexUtils.isSideBySide(x));
    const sortedRegularIndexes = getSortedRegularIndexes(regularIndexes, filter);

    sortedRegularIndexes.forEach((index) => {
        let match = indexMatchesFilter(index, filter, globalIndexingStatus);

        if (!match) {
            // try to match replacement index (if exists)
            const replacement = replacements.find((x) => x.name === IndexUtils.SideBySideIndexPrefix + index.name);
            if (replacement) {
                match = indexMatchesFilter(replacement, filter, globalIndexingStatus);
            }
        }

        if (!match) {
            return;
        }

        const groupName = IndexUtils.getIndexGroupName(index, collections);
        if (!result.has(groupName)) {
            const group: IndexGroup = {
                name: groupName,
                indexes: [],
            };
            result.set(groupName, group);
        }

        const group = result.get(groupName);
        group.indexes.push(index);
    });

    const groups = Array.from(result.values());

    // sort groups
    groups.forEach((group) => {
        group.indexes = getSortedRegularIndexes(group.indexes, filter);
    });

    return {
        regularIndexes: sortedRegularIndexes,
        groups,
        replacements,
    };
}

const anyMatch = (index: IndexSharedInfo, predicate: (index: IndexNodeInfoDetails) => boolean) =>
    index.nodesInfo.some((x) => x.status === "success" && predicate(x.details));

function matchesAnyIndexStatus(
    index: IndexSharedInfo,
    status: IndexStatus[],
    globalIndexingStatus: IndexRunningStatus
): boolean {
    if (status.length === 0) {
        return true;
    }

    /* TODO
        || _.includes(status, "RollingDeployment") && this.rollingDeploymentInProgress()
     */

    return (
        (status.includes("Normal") && anyMatch(index, (x) => IndexUtils.isNormalState(x, globalIndexingStatus))) ||
        (status.includes("ErrorOrFaulty") &&
            (anyMatch(index, IndexUtils.isErrorState) || IndexUtils.hasAnyFaultyNode(index))) ||
        (status.includes("Stale") && anyMatch(index, (x) => x.stale)) ||
        (status.includes("Paused") && anyMatch(index, (x) => IndexUtils.isPausedState(x, globalIndexingStatus))) ||
        (status.includes("Disabled") && anyMatch(index, (x) => IndexUtils.isDisabledState(x, globalIndexingStatus))) ||
        (status.includes("Idle") && anyMatch(index, (x) => IndexUtils.isIdleState(x, globalIndexingStatus)))
    );
}

function matchesIndexType(index: IndexSharedInfo, types: IndexType[]) {
    if (types.length === 0) {
        return true;
    }

    return (
        (types.includes("AutoIndex") && IndexUtils.isAutoIndex(index)) ||
        (types.includes("StaticIndex") && !IndexUtils.isAutoIndex(index))
    );
}

function indexMatchesFilter(
    index: IndexSharedInfo,
    filter: IndexFilterCriteria,
    globalIndexingStatus: IndexRunningStatus
): boolean {
    const nameMatch = !filter.searchText || index.name.toLowerCase().includes(filter.searchText.toLowerCase());
    const statusMatch = matchesAnyIndexStatus(index, filter.statuses, globalIndexingStatus);
    const indexingErrorsMatch =
        !filter.showOnlyIndexesWithIndexingErrors ||
        (filter.showOnlyIndexesWithIndexingErrors && index.nodesInfo.some((x) => x.details?.errorCount > 0));

    const typeMatch = matchesIndexType(index, filter.types);

    return nameMatch && statusMatch && indexingErrorsMatch && typeMatch;
}

function getIndexInfoForDelete(indexes: IndexSharedInfo[]): DeleteIndexesConfirmBodyProps {
    const lockedIndexNames = indexes
        .filter((x) => x.lockMode === "LockedError" || x.lockMode === "LockedIgnore")
        .map((x) => x.name);

    const indexesInfoForDelete = indexes
        .filter((x) => x.lockMode === "Unlock")
        .map((x) => ({
            indexName: x.name,
            reduceOutputCollection: x.reduceOutputCollectionName,
            referenceCollection: x.patternForReferencesToReduceOutputCollection
                ? x.reduceOutputCollectionName + IndexUtils.ReferenceCollectionExtension
                : "",
        }));

    return {
        lockedIndexNames,
        indexesInfoForDelete,
    };
}
