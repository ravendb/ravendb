import genUtils from "common/generalUtils";
import mergedIndexesStorage from "common/storage/mergedIndexesStorage";
import { useAppUrls } from "components/hooks/useAppUrls";
import useBoolean from "components/hooks/useBoolean";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import { milliSecondsInWeek } from "components/utils/common";
import app from "durandal/app";
import database from "models/resources/database";
import moment from "moment";
import router from "plugins/router";
import { useEffect, useRef, useState } from "react";
import { useAsync } from "react-async-hook";
import deleteIndexesConfirm from "viewmodels/database/indexes/deleteIndexesConfirm";

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

interface MergeIndex {
    toMerge: MergeCandidateIndexItem[];
    mergedIndexDefinition: Raven.Client.Documents.Indexes.IndexDefinition;
}

interface UnmergableIndex {
    name: string;
    reason: string;
}

export default function useIndexCleanup(db: database) {
    const [activeTab, setActiveTab] = useState(0);
    const [indexStats, setIndexStats] = useState<IndexStats>(null);

    const [carouselHeight, setCarouselHeight] = useState(null);
    const carouselRefs = useRef<HTMLDivElement[]>(new Array(4).fill(null));

    const [mergableIndexes, setMergableIndexes] = useState<MergeIndex[]>([]);
    const [surpassingIndexes, setSurpassingIndexes] = useState<SurpassingIndex[]>([]);
    const [unusedIndexes, setUnusedIndexes] = useState<UnusedIndex[]>([]);
    const [unmergableIndexes, setUnmergableIndexes] = useState<UnmergableIndex[]>([]);

    const [selectedSurpassingIndexes, setSelectedSurpassingIndexes] = useState<string[]>([]);
    const [selectedUnusedIndexes, setSelectedUnusedIndexes] = useState<string[]>([]);

    const { value: isDeletingSurpassingIndexes, toggle: toggleIsDeletingSurpassingIndexes } = useBoolean(false);
    const { value: isDeletingUnusedIndexes, toggle: toggleIsDeletingUnusedIndexes } = useBoolean(false);

    const { indexesService } = useServices();
    const { appUrl } = useAppUrls();
    const { reportEvent } = useEventsCollector();

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
        const results = await indexesService.getIndexMergeSuggestions(db);

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
    };

    const fetchStats = async () => {
        const locations = db.getLocations();
        const allStats = locations.map((location) => indexesService.getStats(db, location));

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
        const indexesToDelete = indexNames.map((name) => {
            const index = indexStats.get(name);

            return {
                name: index.Name,
                reduceOutputCollectionName: index.ReduceOutputCollection,
                patternForReferencesToReduceOutputCollection: index.PatternReferencesCollectionName,
                lockMode: index.LockMode,
            };
        });

        const deleteIndexesVm = new deleteIndexesConfirm(indexesToDelete, db);
        app.showBootstrapDialog(deleteIndexesVm);
        deleteIndexesVm.deleteTask.done((succeed: boolean, deletedIndexNames: string[]) => {
            if (succeed) {
                filterIndexes(deletedIndexNames);
            }
        });

        await deleteIndexesVm.deleteTask;
    };

    const navigateToMergeSuggestion = (item: MergeIndex) => {
        const mergedIndexName = mergedIndexesStorage.saveMergedIndex(
            db,
            item.mergedIndexDefinition,
            item.toMerge.map((x) => x.name)
        );

        const targetUrl = appUrl.forEditIndex(mergedIndexName, db);

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
    };
}

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
