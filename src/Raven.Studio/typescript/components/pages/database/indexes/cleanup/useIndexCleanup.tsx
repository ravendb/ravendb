import genUtils from "common/generalUtils";
import mergedIndexesStorage from "common/storage/mergedIndexesStorage";
import { useAppUrls } from "components/hooks/useAppUrls";
import useBoolean from "components/hooks/useBoolean";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import { milliSecondsInWeek } from "components/utils/common";
import moment from "moment";
import router from "plugins/router";
import { useEffect, useRef, useState } from "react";
import { useAsync } from "react-async-hook";
import DeleteIndexesConfirmBody, { DeleteIndexesConfirmBodyProps } from "../shared/DeleteIndexesConfirmBody";
import IndexUtils from "components/utils/IndexUtils";
import useConfirm from "components/common/ConfirmDialog";
import React from "react";
import messagePublisher from "common/messagePublisher";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import DatabaseUtils from "components/utils/DatabaseUtils";

type IndexStats = Map<string, Raven.Client.Documents.Indexes.IndexStats>;

interface UnusedIndex {
    name: string;
    containingIndexName?: string;
    lastQueryingTime?: Date;
    lastIndexingTime?: Date;
}

interface SurpassingIndex {
    name: string;
    containingIndexName: string;
    lastQueryingTime?: Date;
    lastIndexingTime?: Date;
}

interface MergeCandidateIndexItem {
    name: string;
    lastQueryTime?: Date;
    lastIndexingTime?: Date;
}

export interface MergeIndex {
    toMerge: MergeCandidateIndexItem[];
    mergedIndexDefinition: Raven.Client.Documents.Indexes.IndexDefinition;
}

interface UnmergableIndex {
    name: string;
    reason: string;
}

export interface MergeSuggestionsError {
    indexName: string;
    message: string;
    stackTrace: string;
}

export default function useIndexCleanup() {
    const db = useAppSelector(databaseSelectors.activeDatabase);

    const [activeTab, setActiveTab] = useState(0);
    const [indexStats, setIndexStats] = useState<IndexStats>(null);

    const [carouselHeight, setCarouselHeight] = useState(null);
    const carouselRefs = useRef<HTMLDivElement[]>(new Array(4).fill(null));

    const [mergableIndexes, setMergableIndexes] = useState<MergeIndex[]>([]);
    const [surpassingIndexes, setSurpassingIndexes] = useState<SurpassingIndex[]>([]);
    const [unusedIndexes, setUnusedIndexes] = useState<UnusedIndex[]>([]);
    const [unmergableIndexes, setUnmergableIndexes] = useState<UnmergableIndex[]>([]);
    const [mergeSuggestionsErrors, setMergeSuggestionsErrors] = useState<MergeSuggestionsError[]>([]);

    const [selectedSurpassingIndexes, setSelectedSurpassingIndexes] = useState<string[]>([]);
    const [selectedUnusedIndexes, setSelectedUnusedIndexes] = useState<string[]>([]);

    const { value: isDeletingSurpassingIndexes, toggle: toggleIsDeletingSurpassingIndexes } = useBoolean(false);
    const { value: isDeletingUnusedIndexes, toggle: toggleIsDeletingUnusedIndexes } = useBoolean(false);

    const { indexesService } = useServices();
    const { appUrl } = useAppUrls();
    const { reportEvent } = useEventsCollector();
    const confirm = useConfirm();

    useEffect(() => {
        if (mergableIndexes.length > 0) {
            return;
        }
        if (surpassingIndexes.length > 0) {
            setActiveTab(1);
            return;
        }
        if (unusedIndexes.length > 0) {
            setActiveTab(2);
            return;
        }
        if (unmergableIndexes.length > 0) {
            setActiveTab(3);
            return;
        }
    }, [mergableIndexes.length, surpassingIndexes.length, unmergableIndexes.length, unusedIndexes.length]);

    const setHeight = (tab: number) => {
        setCarouselHeight(carouselRefs.current[tab].clientHeight);
    };

    const fetchIndexMergeSuggestions = async (indexStats: IndexStats) => {
        const results = await indexesService.getIndexMergeSuggestions(db.name);

        const suggestions = results.Suggestions;
        const mergeCandidatesRaw = suggestions.filter((x) => x.MergedIndex);

        setMergableIndexes(
            mergeCandidatesRaw.map((x) => ({
                mergedIndexDefinition: x.MergedIndex,
                toMerge: x.CanMerge.map((name) => {
                    const stats = indexStats.get(name);
                    return {
                        name,
                        lastQueryingTime: stats.LastQueryingTime ? new Date(stats.LastQueryingTime) : null,
                        lastIndexingTime: stats.LastIndexingTime ? new Date(stats.LastIndexingTime) : null,
                    };
                }),
            }))
        );

        const surpassingRaw = suggestions.filter((x) => !x.MergedIndex);

        const surpassing: SurpassingIndex[] = [];
        surpassingRaw.forEach((group) => {
            group.CanDelete.forEach((deleteCandidate) => {
                const stats = indexStats.get(deleteCandidate);

                surpassing.push({
                    name: deleteCandidate,
                    containingIndexName: group.SurpassingIndex,
                    lastQueryingTime: stats.LastQueryingTime ? new Date(stats.LastQueryingTime) : null,
                    lastIndexingTime: stats.LastIndexingTime ? new Date(stats.LastIndexingTime) : null,
                });
            });
        });

        setSurpassingIndexes(surpassing);

        setUnmergableIndexes(
            Object.keys(results.Unmergables).map((key) => ({
                name: key,
                reason: results.Unmergables[key],
            }))
        );

        setMergeSuggestionsErrors(
            results.Errors.map((x) => ({ indexName: x.IndexName, message: x.Message, stackTrace: x.StackTrace }))
        );
    };

    const fetchStats = async () => {
        const locations = DatabaseUtils.getLocations(db);
        const allStats = locations.map((location) => indexesService.getStats(db.name, location));

        const resultMap = new Map<string, Raven.Client.Documents.Indexes.IndexStats>();

        for await (const nodeStat of allStats) {
            for (const indexStat of nodeStat) {
                const existing = resultMap.get(indexStat.Name);

                resultMap.set(indexStat.Name, {
                    ...indexStat,
                    LastIndexingTime: getNewer(existing?.LastIndexingTime, indexStat.LastIndexingTime),
                    LastQueryingTime: getNewer(existing?.LastQueryingTime, indexStat.LastQueryingTime),
                });
            }
        }

        setIndexStats(resultMap);
        setUnusedIndexes(findUnusedIndexes(resultMap));
        await fetchIndexMergeSuggestions(resultMap);
    };

    // Changing the database causes re-mount
    const asyncFetchStats = useAsync(fetchStats, []);

    const surpassingSelectionState = genUtils.getSelectionState(
        surpassingIndexes.map((x) => x.name),
        selectedSurpassingIndexes
    );

    const toggleAllSurpassingIndexes = () => {
        if (selectedSurpassingIndexes.length === 0) {
            setSelectedSurpassingIndexes(surpassingIndexes.map((x) => x.name));
        } else {
            setSelectedSurpassingIndexes([]);
        }
    };

    const toggleSurpassingIndex = (checked: boolean, indexName: string) => {
        if (checked) {
            setSelectedSurpassingIndexes((selectedIndexes) => [...selectedIndexes, indexName]);
        } else {
            setSelectedSurpassingIndexes((selectedIndexes) => selectedIndexes.filter((x) => x !== indexName));
        }
    };

    const filterSurpassingIndexes = (deletedIndexNames: string[]) => {
        setSelectedSurpassingIndexes((x) => x.filter((y) => !deletedIndexNames.includes(y)));
        setSurpassingIndexes((x) => x.filter((y) => !deletedIndexNames.includes(y.name)));
    };

    const deleteSelectedSurpassingIndexes = async () => {
        try {
            toggleIsDeletingSurpassingIndexes();
            reportEvent("index-merge-suggestions", "delete-surpassing");

            await onDelete(selectedSurpassingIndexes, filterSurpassingIndexes);
        } finally {
            toggleIsDeletingSurpassingIndexes();
        }
    };

    const unusedSelectionState = genUtils.getSelectionState(
        unusedIndexes.map((x) => x.name),
        selectedUnusedIndexes
    );

    const toggleAllUnusedIndexes = () => {
        if (selectedUnusedIndexes.length === 0) {
            setSelectedUnusedIndexes(unusedIndexes.map((x) => x.name));
        } else {
            setSelectedUnusedIndexes([]);
        }
    };

    const toggleUnusedIndex = (checked: boolean, indexName: string) => {
        if (checked) {
            setSelectedUnusedIndexes((selectedIndexes) => [...selectedIndexes, indexName]);
        } else {
            setSelectedUnusedIndexes((selectedIndexes) => selectedIndexes.filter((x) => x !== indexName));
        }
    };

    const filterUnusedIndexes = (deletedIndexNames: string[]) => {
        setSelectedUnusedIndexes((x) => x.filter((y) => !deletedIndexNames.includes(y)));
        setUnusedIndexes((x) => x.filter((y) => !deletedIndexNames.includes(y.name)));
    };

    const deleteSelectedUnusedIndex = async () => {
        try {
            toggleIsDeletingUnusedIndexes();
            reportEvent("index-merge-suggestions", "delete-unused");

            await onDelete(selectedUnusedIndexes, filterUnusedIndexes);
        } finally {
            toggleIsDeletingUnusedIndexes();
        }
    };

    const onDelete = async (indexNames: string[], filterIndexes: (deletedIndexNames: string[]) => void) => {
        const confirmData = getIndexInfoForDelete(indexNames.map((x) => indexStats.get(x)));

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

        filterIndexes(confirmData.indexesInfoForDelete.map((x) => x.indexName));
    };

    const navigateToMergeSuggestion = (item: MergeIndex) => {
        const mergedIndexName = mergedIndexesStorage.saveMergedIndex(
            db.name,
            item.mergedIndexDefinition,
            item.toMerge.map((x) => x.name)
        );

        const targetUrl = appUrl.forEditIndex(mergedIndexName, db.name);

        router.navigate(targetUrl);
    };

    return {
        asyncFetchStats,
        carousel: {
            activeTab,
            setActiveTab,
            setHeight,
            carouselHeight,
            carouselRefs,
        },
        mergable: {
            data: mergableIndexes,
            navigateToMergeSuggestion,
        },
        surpassing: {
            data: surpassingIndexes,
            selectionState: surpassingSelectionState,
            toggle: toggleSurpassingIndex,
            toggleAll: toggleAllSurpassingIndexes,
            selected: selectedSurpassingIndexes,
            deleteSelected: deleteSelectedSurpassingIndexes,
            isDeleting: isDeletingSurpassingIndexes,
        },
        unused: {
            data: unusedIndexes,
            selectionState: unusedSelectionState,
            toggle: toggleUnusedIndex,
            toggleAll: toggleAllUnusedIndexes,
            selected: selectedUnusedIndexes,
            deleteSelected: deleteSelectedUnusedIndex,
            isDeleting: isDeletingUnusedIndexes,
        },
        unmergable: {
            data: unmergableIndexes,
        },
        errors: {
            data: mergeSuggestionsErrors,
        },
    };
}

export type UseIndexCleanupResult = ReturnType<typeof useIndexCleanup>;

function getNewer(date1: string, date2: string) {
    if (!date1) {
        return date2;
    }
    if (!date2) {
        return date1;
    }

    return date1.localeCompare(date2) ? date1 : date2;
}

function findUnusedIndexes(stats: IndexStats): UnusedIndex[] {
    const result: UnusedIndex[] = [];
    const now = moment();

    for (const [name, stat] of stats.entries()) {
        if (stat.LastIndexingTime) {
            const lastQueryDate = moment(stat.LastQueryingTime);
            const agoInMs = now.diff(lastQueryDate);

            if (lastQueryDate.isValid() && agoInMs > milliSecondsInWeek) {
                result.push({
                    name: name,
                    lastQueryingTime: stat.LastQueryingTime ? new Date(stat.LastQueryingTime) : null,
                    lastIndexingTime: stat.LastIndexingTime ? new Date(stat.LastIndexingTime) : null,
                });

                result.sort((a, b) => a.lastQueryingTime.getTime() - b.lastQueryingTime.getTime());
            }
        }
    }

    return result;
}

function getIndexInfoForDelete(indexes: Raven.Client.Documents.Indexes.IndexStats[]): DeleteIndexesConfirmBodyProps {
    const lockedIndexNames = indexes
        .filter((x) => x.LockMode === "LockedError" || x.LockMode === "LockedIgnore")
        .map((x) => x.Name);

    const indexesInfoForDelete = indexes
        .filter((x) => x.LockMode === "Unlock")
        .map((x) => ({
            indexName: x.Name,
            reduceOutputCollection: x.ReduceOutputCollection,
            referenceCollection: x.ReduceOutputReferencePattern
                ? x.ReduceOutputCollection + IndexUtils.ReferenceCollectionExtension
                : "",
        }));

    return {
        lockedIndexNames,
        indexesInfoForDelete,
    };
}

export const formatIndexCleanupDate = (date: Date) => {
    return (
        <>
            {moment.utc(date).local().fromNow()}{" "}
            <small className="text-muted">({moment.utc(date).format("MM/DD/YY, h:mma")})</small>
        </>
    );
};
