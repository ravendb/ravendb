import React, { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import database from "models/resources/database";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import {
    IndexFilterCriteria,
    IndexGroup,
    IndexNodeInfoDetails,
    IndexSharedInfo,
    IndexStatus,
} from "../../../models/indexes";
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import { IndexPanel } from "./IndexPanel";
import deleteIndexesConfirm from "viewmodels/database/indexes/deleteIndexesConfirm";
import app from "durandal/app";
import IndexFilter, { IndexFilterDescription } from "./IndexFilter";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import IndexToolbarActions from "./IndexToolbarActions";
import { useServices } from "../../../hooks/useServices";
import { indexesStatsReducer, indexesStatsReducerInitializer } from "./IndexesStatsReducer";
import collection from "models/database/documents/collection";
import IndexUtils from "../../../utils/IndexUtils";
import genUtils from "common/generalUtils";
import viewHelpers from "common/helpers/view/viewHelpers";
import { CheckboxTriple } from "../../../common/CheckboxTriple";
import { useEventsCollector } from "../../../hooks/useEventsCollector";
import bulkIndexOperationConfirm from "viewmodels/database/indexes/bulkIndexOperationConfirm";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import classNames from "classnames";
import { useAppUrls } from "../../../hooks/useAppUrls";
import { useAccessManager } from "../../../hooks/useAccessManager";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import { shardingTodo } from "common/developmentHelper";
import useTimeout from "../../../hooks/useTimeout";
import useInterval from "../../../hooks/useInterval";
import messagePublisher from "common/messagePublisher";

import "./IndexesPage.scss";

interface IndexesPageProps {
    database: database;
    stale?: boolean;
    indexToHighlight?: string;
}

async function confirmResetIndex(db: database, index: IndexSharedInfo): Promise<boolean> {
    return new Promise((done) => {
        viewHelpers
            .confirmationMessage(
                "Reset index?",
                `You're resetting index: <br><ul><li><strong>${genUtils.escapeHtml(index.name)}</strong></li></ul>
             <div class="margin-top margin-top-lg text-warning bg-warning padding padding-xs flex-horizontal">
                <div class="flex-start">
                    <small><i class="icon-warning"></i></small>
                </div>
                <div>
                    <small>Clicking <strong>Reset</strong> will remove all existing indexed data.</small><br>
                    <small>All items matched by the index definition will be re-indexed.</small>
                </div>
             </div>`,
                {
                    buttons: ["Cancel", "Reset"],
                    html: true,
                }
            )
            .done((result) => {
                done(result.can);
            });
    });
}

interface NoIndexesProps {
    database: database;
}

function NoIndexes(props: NoIndexesProps) {
    const { database } = props;
    const { forCurrentDatabase } = useAppUrls();
    const newIndexUrl = forCurrentDatabase.newIndex();
    const { canReadWriteDatabase } = useAccessManager();

    return (
        <div className="row">
            <div className="col-sm-8 col-sm-offset-2 col-lg-6 col-lg-offset-3">
                <i className="icon-xl icon-empty-set text-muted" />
                <h2 className="text-center text-muted">No indexes have been created for this database.</h2>
                {canReadWriteDatabase(database) && (
                    <p className="lead text-center text-muted">
                        Go ahead and <a href={newIndexUrl}>create one now</a>.
                    </p>
                )}
            </div>
        </div>
    );
}

export const defaultFilterCriteria: IndexFilterCriteria = {
    status: ["Normal", "ErrorOrFaulty", "Stale", "Paused", "Disabled", "Idle", "RollingDeployment"],
    autoRefresh: true,
    showOnlyIndexesWithIndexingErrors: false,
    searchText: "",
};

function matchesAnyIndexStatus(
    index: IndexSharedInfo,
    status: IndexStatus[],
    globalIndexingStatus: IndexRunningStatus
): boolean {
    if (status.length === 0) {
        return false;
    }

    /* TODO
    ADD : _.includes(status, "Stale") && this.isStale()
        || _.includes(status, "RollingDeployment") && this.rollingDeploymentInProgress()
     */

    const anyMatch = (index: IndexSharedInfo, predicate: (index: IndexNodeInfoDetails) => boolean) =>
        index.nodesInfo.some((x) => x.status === "loaded" && predicate(x.details));

    return (
        (status.includes("Normal") && anyMatch(index, (x) => IndexUtils.isNormalState(x, globalIndexingStatus))) ||
        (status.includes("ErrorOrFaulty") &&
            (anyMatch(index, IndexUtils.isErrorState) || IndexUtils.isFaulty(index))) ||
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

function getAllIndexes(groups: IndexGroup[], replacements: IndexSharedInfo[]) {
    const allIndexes: IndexSharedInfo[] = [];
    groups.forEach((group) => allIndexes.push(...group.indexes));
    allIndexes.push(...replacements);
    return allIndexes;
}

export function IndexesPage(props: IndexesPageProps) {
    const { database, stale, indexToHighlight } = props;
    const locations = database.getLocations();

    const { indexesService } = useServices();
    const eventsCollector = useEventsCollector();
    const { canReadWriteDatabase } = useAccessManager();
    const [stats, dispatch] = useReducer(indexesStatsReducer, locations, indexesStatsReducerInitializer);

    shardingTodo("ANY");
    const globalIndexingStatus: IndexRunningStatus = "Running"; //TODO:

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

    const [selectedIndexes, setSelectedIndexes] = useState<string[]>([]);
    const [swapNowProgress, setSwapNowProgress] = useState<string[]>([]);
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
    }, [stats, filter]);

    const fetchProgress = async (location: databaseLocationSpecifier) => {
        const progress = await indexesService.getProgress(database, location);

        dispatch({
            type: "ProgressLoaded",
            progress,
            location,
        });
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
        [database]
    );

    useEffect(() => {
        const nodeTag = clusterTopologyManager.default.localNodeTag();
        const initialLocation = database.getFirstLocation(nodeTag);

        fetchStats(initialLocation);
    }, []);

    useInterval(() => {
        if (filter.autoRefresh) {
            database.getLocations().forEach((location) => fetchProgress(location));
        }
    }, 10_000);

    const highlightUsed = useRef<boolean>(false);

    const highlightCallback = useCallback((node: HTMLElement) => {
        if (node && !highlightUsed.current) {
            node.scrollIntoView({ behavior: "smooth" });
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

    const deleteSelectedIndexes = () => {
        eventsCollector.reportEvent("indexes", "delete-selected");
        return confirmDeleteIndexes(database, getSelectedIndexes());
    };

    const disableIndexes = async (
        enableIndex: boolean,
        indexes: IndexSharedInfo[],
        locations: databaseLocationSpecifier[]
    ) => {
        eventsCollector.reportEvent("index", "toggle-status", status);

        const locationsToApply = [...locations];

        while (locationsToApply.length > 0) {
            const location = locationsToApply.pop();

            const indexesToApply = [...indexes];
            while (indexesToApply.length > 0) {
                const index = indexesToApply.pop();
                if (enableIndex) {
                    await indexesService.enable(index, database, location);
                } else {
                    await indexesService.disable(index, database, location);
                }

                dispatch({
                    type: enableIndex ? "EnableIndexing" : "DisableIndexing",
                    indexName: index.name,
                    location,
                });
            }
        }
    };

    const toggleDisableIndexes = useCallback(async (enableIndex: boolean, indexes: IndexSharedInfo[]) => {
        const locations = database.getLocations();
        const confirmation = enableIndex
            ? bulkIndexOperationConfirm.forEnable(indexes, locations)
            : bulkIndexOperationConfirm.forDisable(indexes, locations);

        confirmation.result.done((result) => {
            if (result.can) {
                disableIndexes(enableIndex, indexes, result.locations);
            }
        });

        app.showBootstrapDialog(confirmation);
    }, []);

    const enableSelectedIndexes = useCallback(
        () => toggleDisableIndexes(true, getSelectedIndexes()),
        [getSelectedIndexes]
    );

    const disableSelectedIndexes = useCallback(
        () => toggleDisableIndexes(false, getSelectedIndexes()),
        [getSelectedIndexes]
    );

    const pauseIndexes = async (
        resume: boolean,
        indexes: IndexSharedInfo[],
        locations: databaseLocationSpecifier[]
    ) => {
        eventsCollector.reportEvent("index", "toggle-status", status);

        const locationsToApply = [...locations];

        while (locationsToApply.length > 0) {
            const location = locationsToApply.pop();

            const indexesToApply = [...indexes];
            while (indexesToApply.length > 0) {
                const index = indexesToApply.pop();
                if (resume) {
                    await indexesService.resume(index, database, location);
                } else {
                    await indexesService.pause(index, database, location);
                }

                dispatch({
                    type: resume ? "ResumeIndexing" : "PauseIndexing",
                    indexName: index.name,
                    location,
                });
            }
        }
    };

    const togglePauseIndexes = useCallback(async (resume: boolean, indexes: IndexSharedInfo[]) => {
        const locations = database.getLocations();
        const confirmation = resume
            ? bulkIndexOperationConfirm.forResume(indexes, locations)
            : bulkIndexOperationConfirm.forPause(indexes, locations);

        confirmation.result.done((result) => {
            if (result.can) {
                pauseIndexes(resume, indexes, result.locations);
            }
        });

        app.showBootstrapDialog(confirmation);
    }, []);

    const resumeSelectedIndexes = useCallback(
        () => togglePauseIndexes(true, getSelectedIndexes()),
        [getSelectedIndexes]
    );

    const pauseSelectedIndexes = useCallback(
        () => togglePauseIndexes(false, getSelectedIndexes()),
        [getSelectedIndexes]
    );

    const setLockModeSelectedIndexes = async (lockMode: IndexLockMode, indexes: IndexSharedInfo[]) => {
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
    };

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
        [selectedIndexes, stats, setLockModeSelectedIndexes, getSelectedIndexes]
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

    const setIndexLockModeInternal = useCallback(
        async (index: IndexSharedInfo, lockMode: IndexLockMode) => {
            await indexesService.setLockMode([index], lockMode, database);

            dispatch({
                type: "SetLockMode",
                lockMode,
                indexName: index.name,
            });
        },
        [database]
    );

    const setIndexLockMode = useCallback(
        async (index: IndexSharedInfo, lockMode: IndexLockMode) => {
            await setIndexLockModeInternal(index, lockMode);
            messagePublisher.reportSuccess("Lock mode was set to: " + IndexUtils.formatLockMode(lockMode));
        },
        [setIndexLockModeInternal]
    );

    const loadMissing = async () => {
        const tasks = stats.indexes[0].nodesInfo.map(async (nodeInfo) => {
            if (nodeInfo.status === "notLoaded") {
                await fetchStats(nodeInfo.location);
            }
            await fetchProgress(nodeInfo.location);
        });

        await Promise.all(tasks);
    };

    useTimeout(loadMissing, 3_000);

    const toggleSelection = (index: IndexSharedInfo) => {
        setSelectedIndexes(s => {
            if (s.includes(index.name)) {
                return s.filter((x) => x !== index.name);
            } else {
                return s.concat(index.name);
            }
        })
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
        const canReset = await confirmResetIndex(database, index);
        if (canReset) {
            eventsCollector.reportEvent("indexes", "reset");

            const locations = database.getLocations();
            while (locations.length) {
                await indexesService.resetIndex(index, database, locations.pop());
            }

            messagePublisher.reportSuccess("Index " + index.name + " successfully reset");

            /* TODO
             // reset index is implemented as delete and insert, so we receive notification about deleted index via changes API
                    // let's issue marker to ignore index delete information for next few seconds because it might be caused by reset.
                    // Unfortunately we can't use resetIndexVm.resetTask.done, because we receive event via changes api before resetTask promise 
                    // is resolved. 
                    this.resetsInProgress.add(i.name);

                    new resetIndexCommand(i.name, this.activeDatabase())
                        .execute();

                    setTimeout(() => {
                        this.resetsInProgress.delete(i.name);
                    }, 30000);
             */
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

        /* TODO:
        const replacementIndex = irdx.replacement();
        if (replacementIndex.progress() && replacementIndex.progress().rollingProgress().length) {
            text += `<li ${margin}>Actual indexing will occur once the node reaches its turn in the rolling deployment process.</li>`;
        }*/

        //TODO: is it node wide?

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

    const indexesSelectionState = (): checkbox => {
        const selectedCount = selectedIndexes.length;
        const indexesCount = getAllIndexes(groups, replacements).length;
        if (indexesCount && selectedCount === indexesCount) {
            return "checked";
        }
        if (selectedCount > 0) {
            return "some_checked";
        }
        return "unchecked";
    };

    if (stats.indexes.length === 0) {
        return <NoIndexes database={database} />;
    }

    return (
        <div className="flex-vertical absolute-fill">
            <div className="flex-header">
                {stats.indexes.length > 0 && (
                    <div className="clearfix toolbar">
                        <div className="pull-left">
                            <div className="form-inline">
                                {canReadWriteDatabase(database) && (
                                    <div
                                        className="checkbox checkbox-primary checkbox-inline align-checkboxes"
                                        title="Select all or none"
                                    >
                                        <CheckboxTriple onChanged={toggleSelectAll} state={indexesSelectionState()} />
                                        <label />
                                    </div>
                                )}

                                <IndexFilter filter={filter} setFilter={setFilter} />
                                {canReadWriteDatabase(database) && (
                                    <IndexToolbarActions
                                        selectedIndexes={selectedIndexes}
                                        deleteSelectedIndexes={deleteSelectedIndexes}
                                        enableSelectedIndexes={enableSelectedIndexes}
                                        disableSelectedIndexes={disableSelectedIndexes}
                                        pauseSelectedIndexes={pauseSelectedIndexes}
                                        resumeSelectedIndexes={resumeSelectedIndexes}
                                        setLockModeSelectedIndexes={confirmSetLockModeSelectedIndexes}
                                    />
                                )}
                            </div>
                        </div>
                        {/*  TODO  <IndexGlobalIndexing /> */}
                    </div>
                )}
                <IndexFilterDescription filter={filter} indexes={getAllIndexes(groups, replacements)} />
            </div>
            <div className="flex-grow scroll js-scroll-container">
                {groups.map((group) => {
                    return (
                        <div key={"group-" + group.name}>
                            <h2 className="on-base-background" title={"Collection: " + group.name}>
                                {group.name}
                            </h2>
                            {group.indexes.map((index) => {
                                const replacement = replacements.find(
                                    (x) => x.name === IndexUtils.SideBySideIndexPrefix + index.name
                                );
                                return (
                                    <React.Fragment key={index.name}>
                                        <IndexPanel
                                            setPriority={(p) => setIndexPriority(index, p)}
                                            setLockMode={(l) => setIndexLockMode(index, l)}
                                            globalIndexingStatus={globalIndexingStatus}
                                            resetIndex={() => resetIndex(index)}
                                            openFaulty={(location: databaseLocationSpecifier) =>
                                                openFaulty(index, location)
                                            }
                                            enableIndexing={() => toggleDisableIndexes(true, [index])}
                                            disableIndexing={() => toggleDisableIndexes(false, [index])}
                                            pauseIndexing={() => togglePauseIndexes(false, [index])}
                                            resumeIndexing={() => togglePauseIndexes(true, [index])}
                                            index={index}
                                            hasReplacement={!!replacement}
                                            database={database}
                                            deleteIndex={() => confirmDeleteIndexes(database, [index])}
                                            selected={selectedIndexes.includes(index.name)}
                                            toggleSelection={() => toggleSelection(index)}
                                            key={index.name}
                                            ref={indexToHighlight === index.name ? highlightCallback : undefined}
                                        />
                                        {replacement && (
                                            <div className="sidebyside-actions">
                                                <div className="panel panel-state panel-warning">
                                                    <div className="state state-swap">
                                                        <i className="icon-swap" />
                                                    </div>
                                                    <div className="padding flex-horizontal">
                                                        <div className="title">Side by side</div>
                                                        <div className="flex-separator" />
                                                        <button
                                                            className={classNames("btn btn-sm btn-warning", {
                                                                "btn-spinner": swapNowProgress.includes(index.name),
                                                            })}
                                                            disabled={swapNowProgress.includes(index.name)}
                                                            onClick={() => confirmSwapSideBySide(index)}
                                                            title="Click to replace the current index definition with the replacement index"
                                                        >
                                                            <i className="icon-force" /> <span>Swap now</span>
                                                        </button>
                                                    </div>
                                                </div>
                                            </div>
                                        )}
                                        {replacement && (
                                            <IndexPanel
                                                setPriority={(p) => setIndexPriority(replacement, p)}
                                                setLockMode={(l) => setIndexLockMode(replacement, l)}
                                                globalIndexingStatus={globalIndexingStatus}
                                                resetIndex={() => resetIndex(replacement)}
                                                openFaulty={(location: databaseLocationSpecifier) =>
                                                    openFaulty(replacement, location)
                                                }
                                                enableIndexing={() => toggleDisableIndexes(true, [replacement])}
                                                disableIndexing={() => toggleDisableIndexes(false, [replacement])}
                                                pauseIndexing={() => togglePauseIndexes(false, [replacement])}
                                                resumeIndexing={() => togglePauseIndexes(true, [replacement])}
                                                index={replacement}
                                                database={database}
                                                deleteIndex={() => confirmDeleteIndexes(database, [replacement])}
                                                selected={selectedIndexes.includes(replacement.name)}
                                                toggleSelection={() => toggleSelection(replacement)}
                                                key={replacement.name}
                                            />
                                        )}
                                    </React.Fragment>
                                );
                            })}
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
