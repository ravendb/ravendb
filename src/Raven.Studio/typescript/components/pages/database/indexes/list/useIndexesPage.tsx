import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import { BulkIndexOperationConfirm } from "components/pages/database/indexes/list/BulkIndexOperationConfirm";
import {
    IndexFilterCriteria,
    IndexGroup,
    IndexNodeInfoDetails,
    IndexSharedInfo,
    IndexStatus,
} from "components/models/indexes";
import database from "models/resources/database";
import {
    indexesStatsReducer,
    indexesStatsReducerInitializer,
} from "components/pages/database/indexes/list/IndexesStatsReducer";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import useInterval from "hooks/useInterval";
import messagePublisher from "common/messagePublisher";
import IndexUtils from "components/utils/IndexUtils";
import viewHelpers from "common/helpers/view/viewHelpers";
import genUtils from "common/generalUtils";
import deleteIndexesConfirm from "viewmodels/database/indexes/deleteIndexesConfirm";
import app from "durandal/app";
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
import IndexChange = Raven.Client.Documents.Changes.IndexChange;

export function useIndexesPage(database: database, stale: boolean) {
    //TODO: use DatabaseSharedInfo?
    const locations = database.getLocations();

    const [bulkOperationConfirm, setBulkOperationConfirm] = useState<{
        type: Parameters<typeof BulkIndexOperationConfirm>[0]["type"];
        indexes: IndexSharedInfo[];
        locations: databaseLocationSpecifier[];
        onConfirm: (locations: databaseLocationSpecifier[]) => void;
    }>();

    const { indexesService } = useServices();
    const eventsCollector = useEventsCollector();
    const { databaseChangesApi } = useChanges();

    const [resetIndexConfirm, setResetIndexConfirm] = useState<{
        index: IndexSharedInfo;
    }>();

    const [stats, dispatch] = useReducer(indexesStatsReducer, locations, indexesStatsReducerInitializer);

    const [filter, setFilter] = useState<IndexFilterCriteria>(() => {
        if (stale) {
            return {
                ...defaultFilterCriteria,
                status: ["Stale"],
            };
        } else {
            return defaultFilterCriteria;
        }
    });

    shardingTodo("ANY");
    const globalIndexingStatus: IndexRunningStatus = "Running"; //TODO:

    const [selectedIndexes, setSelectedIndexes] = useState<string[]>([]);
    const [swapNowProgress, setSwapNowProgress] = useState<string[]>([]);

    //TODO:
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [globalLockChanges, setGlobalLockChanges] = useState(false);

    const { groups, replacements } = useMemo(() => {
        const collections = collectionsTracker.default.collections();
        const groupedIndexes = groupAndFilterIndexStats(stats.indexes, collections, filter, globalIndexingStatus);

        const allVisibleIndexes = getAllIndexes(groupedIndexes.groups, groupedIndexes.replacements);
        const newSelection = selectedIndexes.filter((x) => allVisibleIndexes.some((idx) => idx.name === x));
        if (newSelection.length !== selectedIndexes.length) {
            setSelectedIndexes(newSelection);
        }

        return groupedIndexes;
    }, [stats, filter, selectedIndexes]);

    const fetchProgress = async (location: databaseLocationSpecifier) => {
        try {
            const progress = await indexesService.getProgress(database, location);

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
            const stats = await indexesService.getStats(database, location);
            dispatch({
                type: "StatsLoaded",
                location,
                stats,
            });
        },
        [database, indexesService]
    );

    const throttledRefresh = useRef(
        _.throttle(() => {
            database.getLocations().forEach(fetchStats);
        }, 3_000)
    );

    const throttledProgressRefresh = useRef(
        _.throttle(() => {
            database.getLocations().forEach((location) => fetchProgress(location));
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
        const tasks = database.getLocations().map(fetchStats);
        try {
            await Promise.race(tasks);
        } catch {
            // ignore - we handle that below
        }

        Promise.all(tasks).finally(() => throttledProgressRefresh.current());
    }, []);

    const currentProcessor = useRef<(e: IndexChange) => void>();

    useEffect(() => {
        currentProcessor.current = (e: Raven.Client.Documents.Changes.IndexChange) => {
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

    useEffect(() => {
        if (databaseChangesApi) {
            const watch = databaseChangesApi.watchAllIndexes((e) => currentProcessor.current(e));

            return () => {
                watch.off();
            };
        }
    }, [databaseChangesApi]);

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
        async (indexes: IndexSharedInfo[], locations: databaseLocationSpecifier[]) => {
            eventsCollector.reportEvent("index", "toggle-status");

            for (let locationIdx = 0; locationIdx < locations.length; locationIdx++) {
                for (let indexIdx = 0; indexIdx < indexes.length; indexIdx++) {
                    const location = locations[locationIdx];
                    const index = indexes[indexIdx];

                    const indexStatus = index.nodesInfo.find((x) => _.isEqual(x.location, location))?.details?.status;

                    if (indexStatus === "Disabled") {
                        continue;
                    }

                    await indexesService.disable(index, database, location);

                    dispatch({
                        type: "DisableIndexing",
                        indexName: index.name,
                        location,
                    });
                }
            }
        },
        [indexesService, database, eventsCollector]
    );

    const disableIndexes = useCallback(
        async (indexes: IndexSharedInfo[]) => {
            setBulkOperationConfirm({
                type: "disable",
                indexes,
                locations: database.getLocations(),
                onConfirm: (locations: databaseLocationSpecifier[]) => onDisableIndexesConfirm(indexes, locations),
            });
        },
        [setBulkOperationConfirm, onDisableIndexesConfirm, database]
    );

    const onPauseIndexesConfirm = useCallback(
        async (indexes: IndexSharedInfo[], locations: databaseLocationSpecifier[]) => {
            eventsCollector.reportEvent("index", "toggle-status");

            for (let locationIdx = 0; locationIdx < locations.length; locationIdx++) {
                for (let indexIdx = 0; indexIdx < indexes.length; indexIdx++) {
                    const location = locations[locationIdx];
                    const index = indexes[indexIdx];

                    const indexStatus = index.nodesInfo.find((x) => _.isEqual(x.location, location))?.details?.status;

                    if (indexStatus === "Paused") {
                        continue;
                    }

                    await indexesService.pause(index, database, location);

                    dispatch({
                        type: "PauseIndexing",
                        indexName: index.name,
                        location,
                    });
                }
            }
        },
        [eventsCollector, indexesService, database]
    );

    const pauseIndexes = useCallback(
        async (indexes: IndexSharedInfo[]) => {
            setBulkOperationConfirm({
                type: "pause",
                indexes,
                locations: database.getLocations(),
                onConfirm: (locations: databaseLocationSpecifier[]) => onPauseIndexesConfirm(indexes, locations),
            });
        },
        [setBulkOperationConfirm, onPauseIndexesConfirm, database]
    );

    const onStartIndexesConfirm = useCallback(
        async (indexes: IndexSharedInfo[], locations: databaseLocationSpecifier[]) => {
            eventsCollector.reportEvent("index", "toggle-status");

            for (let locationIdx = 0; locationIdx < locations.length; locationIdx++) {
                for (let indexIdx = 0; indexIdx < indexes.length; indexIdx++) {
                    const location = locations[locationIdx];
                    const index = indexes[indexIdx];

                    const indexStatus = index.nodesInfo.find((x) => _.isEqual(x.location, location))?.details?.status;

                    if (indexStatus !== "Disabled" && indexStatus !== "Paused") {
                        continue;
                    }

                    if (indexStatus === "Disabled") {
                        await indexesService.enable(index, database, location);
                    } else {
                        await indexesService.resume(index, database, location);
                    }

                    dispatch({
                        type: indexStatus === "Disabled" ? "EnableIndexing" : "ResumeIndexing",
                        indexName: index.name,
                        location,
                    });
                }
            }
        },
        [eventsCollector, indexesService, database]
    );

    const startIndexes = useCallback(
        async (indexes: IndexSharedInfo[]) => {
            setBulkOperationConfirm({
                type: "start",
                indexes,
                locations: database.getLocations(),
                onConfirm: (locations: databaseLocationSpecifier[]) => onStartIndexesConfirm(indexes, locations),
            });
        },
        [database, onStartIndexesConfirm]
    );

    const setIndexLockModeInternal = useCallback(
        async (index: IndexSharedInfo, lockMode: IndexLockMode) => {
            await indexesService.setLockMode([index], lockMode, database);

            dispatch({
                type: "SetLockMode",
                lockMode,
                indexName: index.name,
            });
        },
        [database, indexesService]
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
            const lockModeFormatted = IndexUtils.formatLockMode(lockMode);

            const indexes = getSelectedIndexes().filter(
                (index) => index.type !== "AutoMap" && index.type !== "AutoMapReduce"
            );

            viewHelpers
                .confirmationMessage(
                    "Are you sure?",
                    `Do you want to <strong>${genUtils.escapeHtml(
                        lockModeFormatted
                    )}</strong> selected indexes?</br>Note: Static-indexes only will be set, 'Lock Mode' is not relevant for auto-indexes.`,
                    {
                        html: true,
                    }
                )
                .done((can) => {
                    if (can) {
                        setLockModeSelectedIndexes(lockMode, indexes);
                    }
                });
        },
        [setLockModeSelectedIndexes, getSelectedIndexes]
    );

    const confirmDeleteIndexes = async (db: database, indexes: IndexSharedInfo[]): Promise<void> => {
        eventsCollector.reportEvent("indexes", "delete");
        if (indexes.length > 0) {
            const deleteIndexesVm = new deleteIndexesConfirm(indexes, db);
            app.showBootstrapDialog(deleteIndexesVm);
            deleteIndexesVm.deleteTask.done((deleted: boolean) => {
                if (deleted) {
                    dispatch({
                        type: "DeleteIndexes",
                        indexNames: indexes.map((x) => x.name),
                    });
                }
            });
            await deleteIndexesVm.deleteTask;
        }
    };

    const setIndexPriority = async (index: IndexSharedInfo, priority: IndexPriority) => {
        await indexesService.setPriority(index, priority, database);

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
        viewHelpers
            .confirmationMessage(
                "Open index?",
                `You're opening a faulty index <strong>'${genUtils.escapeHtml(index.name)}'</strong>`,
                {
                    html: true,
                }
            )
            .done((result) => {
                if (result.can) {
                    eventsCollector.reportEvent("indexes", "open");

                    indexesService.openFaulty(index, database, location);
                }
            });
    };

    const resetIndex = async (index: IndexSharedInfo) => {
        setResetIndexConfirm({
            index,
        });
    };

    const onResetIndexConfirm = async (index: IndexSharedInfo) => {
        eventsCollector.reportEvent("indexes", "reset");

        setResettingIndex(true);

        try {
            const locations = database.getLocations();

            while (locations.length) {
                const location = locations.pop();

                dispatch({
                    type: "ResetIndex",
                    indexName: index.name,
                    location,
                });
                await indexesService.resetIndex(index, database, location);
            }

            messagePublisher.reportSuccess("Index " + index.name + " successfully reset");
        } finally {
            // wait a bit and trigger refresh
            await delay(1_000);

            throttledRefresh.current();
            setResettingIndex(false);
        }
    };

    const swapSideBySide = async (index: IndexSharedInfo) => {
        setSwapNowProgress((x) => [...x, index.name]);
        eventsCollector.reportEvent("index", "swap-side-by-side");
        try {
            await indexesService.forceReplace(index.name, database);
        } finally {
            setSwapNowProgress((item) => item.filter((x) => x !== index.name));
        }
    };

    const confirmSwapSideBySide = (index: IndexSharedInfo) => {
        const margin = `class="margin-bottom"`;
        let text = `<li ${margin}>Index: <strong>${genUtils.escapeHtml(index.name)}</strong></li>`;
        text += `<li ${margin}>Clicking <strong>Swap Now</strong> will immediately replace the current index definition with the replacement index.</li>`;

        viewHelpers
            .confirmationMessage("Are you sure?", `<ul>${text}</ul>`, { buttons: ["Cancel", "Swap Now"], html: true })
            .done((result: canActivateResultDto) => {
                if (result.can) {
                    swapSideBySide(index);
                }
            });
    };

    const toggleSelectAll = () => {
        eventsCollector.reportEvent("indexes", "toggle-select-all");
        const selectedIndexesCount = selectedIndexes.length;

        if (selectedIndexesCount > 0) {
            setSelectedIndexes([]);
        } else {
            const toSelect: string[] = [];
            groups.forEach((group) => {
                toSelect.push(...group.indexes.map((x) => x.name));
            });
            toSelect.push(...replacements.map((x) => x.name));
            setSelectedIndexes(toSelect);
        }
    };

    const indexesCount = getAllIndexes(groups, replacements).length;

    return {
        loading,
        bulkOperationConfirm,
        setBulkOperationConfirm,
        resetIndexConfirm,
        setResetIndexConfirm,
        stats,
        selectedIndexes,
        toggleSelectAll,
        filter,
        setFilter,
        groups,
        replacements,
        swapNowProgress,
        highlightCallback,
        confirmSwapSideBySide,
        confirmSetLockModeSelectedIndexes,
        indexesCount,
        setIndexPriority,
        getSelectedIndexes,
        startIndexes,
        disableIndexes,
        pauseIndexes,
        setIndexLockMode,
        resetIndex,
        toggleSelection,
        onResetIndexConfirm,
        openFaulty,
        confirmDeleteIndexes,
        globalIndexingStatus,
    };
}

export const defaultFilterCriteria: IndexFilterCriteria = {
    status: ["Normal", "ErrorOrFaulty", "Stale", "Paused", "Disabled", "Idle", "RollingDeployment"],
    autoRefresh: true,
    showOnlyIndexesWithIndexingErrors: false,
    searchText: "",
};

export function getAllIndexes(groups: IndexGroup[], replacements: IndexSharedInfo[]) {
    const allIndexes: IndexSharedInfo[] = [];
    groups.forEach((group) => allIndexes.push(...group.indexes));
    allIndexes.push(...replacements);
    return allIndexes;
}

function groupAndFilterIndexStats(
    indexes: IndexSharedInfo[],
    collections: collection[],
    filter: IndexFilterCriteria,
    globalIndexingStatus: IndexRunningStatus
): { groups: IndexGroup[]; replacements: IndexSharedInfo[] } {
    const result = new Map<string, IndexGroup>();

    const replacements = indexes.filter(IndexUtils.isSideBySide);
    const regularIndexes = indexes.filter((x) => !IndexUtils.isSideBySide(x));

    regularIndexes.forEach((index) => {
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

    // sort groups
    const groups = Array.from(result.values());
    groups.sort((l, r) => genUtils.sortAlphaNumeric(l.name, r.name));

    groups.forEach((group) => {
        group.indexes.sort((a, b) => genUtils.sortAlphaNumeric(a.name, b.name));
    });

    return {
        groups,
        replacements,
    };
}

function matchesAnyIndexStatus(
    index: IndexSharedInfo,
    status: IndexStatus[],
    globalIndexingStatus: IndexRunningStatus
): boolean {
    if (status.length === 0) {
        return false;
    }

    /* TODO
        || _.includes(status, "RollingDeployment") && this.rollingDeploymentInProgress()
     */

    const anyMatch = (index: IndexSharedInfo, predicate: (index: IndexNodeInfoDetails) => boolean) =>
        index.nodesInfo.some((x) => x.status === "success" && predicate(x.details));

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

function indexMatchesFilter(
    index: IndexSharedInfo,
    filter: IndexFilterCriteria,
    globalIndexingStatus: IndexRunningStatus
): boolean {
    const nameMatch = !filter.searchText || index.name.toLowerCase().includes(filter.searchText.toLowerCase());
    const statusMatch = matchesAnyIndexStatus(index, filter.status, globalIndexingStatus);
    const indexingErrorsMatch =
        !filter.showOnlyIndexesWithIndexingErrors ||
        (filter.showOnlyIndexesWithIndexingErrors && index.nodesInfo.some((x) => x.details?.errorCount > 0));

    return nameMatch && statusMatch && indexingErrorsMatch;
}
