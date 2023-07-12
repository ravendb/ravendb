import React, { useEffect, useRef, useState } from "react";
import { Icon } from "components/common/Icon";
import { Badge, Button, Card, Carousel, CarouselItem, Nav, NavItem, Table } from "reactstrap";
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
import { Checkbox } from "components/common/Checkbox";
import moment from "moment";
import { EmptySet } from "components/common/EmptySet";
import classNames from "classnames";
import database from "models/resources/database";
import { useServices } from "components/hooks/useServices";
import { milliSecondsInWeek } from "components/utils/common";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import deleteIndexesConfirm from "viewmodels/database/indexes/deleteIndexesConfirm";
import app from "durandal/app";
import { useAppUrls } from "components/hooks/useAppUrls";
import genUtils from "common/generalUtils";
import mergedIndexesStorage from "common/storage/mergedIndexesStorage";
import router from "plugins/router";

const mergeIndexesImg = require("Content/img/pages/indexCleanup/merge-indexes.svg");
const removeSubindexesImg = require("Content/img/pages/indexCleanup/remove-subindexes.svg");
const removeUnusedImg = require("Content/img/pages/indexCleanup/remove-unused.svg");
const unmergableIndexesImg = require("Content/img/pages/indexCleanup/unmergable-indexes.svg");

type IndexStats = Map<string, Raven.Client.Documents.Indexes.IndexStats>;

interface UnusedIndexInfo {
    name: string;
    containingIndexName?: string;
    lastQueryingTime?: Date;
    lastIndexingTime?: Date;
}

interface SurpassingIndexInfo {
    name: string;
    containingIndexName: string;
    lastQueryingTime?: Date;
    lastIndexingTime?: Date;
}

interface MergeCandidateIndexItemInfo {
    name: string;
    lastQueryTime?: Date;
    lastIndexingTime?: Date;
}

interface MergeIndexInfo {
    toMerge: MergeCandidateIndexItemInfo[];
    mergedIndexDefinition: Raven.Client.Documents.Indexes.IndexDefinition;
}

interface UnmergableIndexInfo {
    name: string;
    reason: string;
}

interface IndexCleanupProps {
    db: database;
}

export function IndexCleanup(props: IndexCleanupProps) {
    const { db } = props;

    // ------------------------------------------------------
    // TODO kalczur move to custom hook or redux
    const [indexStats, setIndexStats] = useState<IndexStats>(null);

    const [mergableIndexes, setMergableIndexes] = useState<MergeIndexInfo[]>([]);
    const [surpassingIndexes, setSurpassingIndexes] = useState<SurpassingIndexInfo[]>([]);
    const [unusedIndexes, setUnusedIndexes] = useState<UnusedIndexInfo[]>([]);
    const [unmergableIndexes, setUnmergableIndexes] = useState<UnmergableIndexInfo[]>([]);

    const [selectedSurpassingIndexes, setSelectedSurpassingIndexes] = useState<string[]>([]);
    const [selectedUnusedIndexes, setSelectedUnusedIndexes] = useState<string[]>([]);

    const { indexesService } = useServices();
    const { appUrl } = useAppUrls();
    const { reportEvent } = useEventsCollector();

    const getNewer = (date1: string, date2: string) => {
        if (!date1) {
            return date2;
        }
        if (!date2) {
            return date1;
        }

        return date1.localeCompare(date2) ? date1 : date2;
    };

    const findUnusedIndexes = (stats: IndexStats): UnusedIndexInfo[] => {
        const result: UnusedIndexInfo[] = [];
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

        const surpassing: SurpassingIndexInfo[] = [];
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

    useEffect(() => {
        (async () => {
            await fetchStats();
        })();
    }, []);

    // ------

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
        reportEvent("index-merge-suggestions", "delete-surpassing");
        await onDelete(selectedSurpassingIndexes, filterSurpassingIndexes);
    };

    // -----------------

    const unusedSelectionState = genUtils.getSelectionState(
        surpassingIndexes.map((x) => x.name),
        selectedSurpassingIndexes
    );

    const toggleAllUnusedIndexes = () => {
        if (selectedSurpassingIndexes.length === 0) {
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
        reportEvent("index-merge-suggestions", "delete-unused");
        await onDelete(selectedUnusedIndexes, filterUnusedIndexes);
    };

    const onDelete = async (indexNames: string[], filterIndexes: (x: string[]) => void) => {
        const indexes = indexNames.map((x) => {
            const a = indexStats.get(x);

            return {
                name: a.Name,
                reduceOutputCollectionName: a.ReduceOutputCollection,
                patternForReferencesToReduceOutputCollection: a.PatternReferencesCollectionName,
                lockMode: a.LockMode,
            };
        });

        const deleteIndexesVm = new deleteIndexesConfirm(indexes, db);
        app.showBootstrapDialog(deleteIndexesVm);
        deleteIndexesVm.deleteTask.done((succeed: boolean, deletedIndexNames: string[]) => {
            if (succeed) {
                filterIndexes(deletedIndexNames);
            }
        });

        await deleteIndexesVm.deleteTask;
    };

    const navigateToMergeSuggestion = (item: MergeIndexInfo) => {
        const mergedIndexName = mergedIndexesStorage.saveMergedIndex(
            db,
            item.mergedIndexDefinition,
            item.toMerge.map((x) => x.name)
        );

        const targetUrl = appUrl.forEditIndex(mergedIndexName, db);

        router.navigate(targetUrl);
    };

    // ------------------------------------------------------

    function activeNonEmpty() {
        if (mergableIndexes.length !== 0) return 0;
        if (surpassingIndexes.length !== 0) return 1;
        if (unusedIndexes.length !== 0) return 2;
        if (unmergableIndexes.length !== 0) return 3;
        return 0;
    }

    const [currentActiveTab, setCurrentActiveTab] = useState(activeNonEmpty());
    const [carouselHeight, setCarouselHeight] = useState(null);
    const carouselRefs = useRef([]);

    const setHeight = () => {
        const activeCarouselItem = carouselRefs.current[currentActiveTab];
        if (activeCarouselItem) {
            setCarouselHeight(activeCarouselItem.clientHeight);
        }
    };

    const toggleTab = (tab: number) => {
        if (currentActiveTab !== tab) {
            setHeight();
            setCurrentActiveTab(tab);
        }
    };

    const onCarouselExited = () => {
        setCarouselHeight(null);
    };

    return (
        <div className="p-4">
            <h2 className="mb-4">
                <Icon icon="clean" /> Index Cleanup
            </h2>
            <div className="text-limit-width mb-5">
                <p>
                    Maintaining multiple indexes can lower performance. Every time data is inserted, updated, or
                    deleted, the corresponding indexes need to be updated as well, which can lead to increased write
                    latency.
                </p>
                <p>
                    To counter these performance issues, RavenDB recommends a set of actions to optimize the number of
                    indexes. Note that you need to update the index reference in your application.
                </p>
            </div>

            <Nav className="card-tabs gap-3 card-tabs">
                <NavItem>
                    <Card
                        className={classNames("p-3", "card-tab", { active: currentActiveTab === 0 })}
                        onClick={() => toggleTab(0)}
                    >
                        <img src={mergeIndexesImg} alt="" />
                        <Badge
                            className="rounded-pill fs-5"
                            color={mergableIndexes.length !== 0 ? "primary" : "secondary"}
                        >
                            {mergableIndexes.length}
                        </Badge>
                        <h4 className="text-center">
                            Merge
                            <br />
                            indexes
                        </h4>
                    </Card>
                </NavItem>
                <NavItem>
                    <Card
                        className={classNames("p-3", "card-tab", { active: currentActiveTab === 1 })}
                        onClick={() => toggleTab(1)}
                    >
                        <img src={removeSubindexesImg} alt="" />
                        <Badge
                            className="rounded-pill fs-5"
                            color={surpassingIndexes.length !== 0 ? "primary" : "secondary"}
                        >
                            {surpassingIndexes.length}
                        </Badge>
                        <h4 className="text-center">
                            Remove
                            <br />
                            sub-indexes
                        </h4>
                    </Card>
                </NavItem>
                <NavItem>
                    <Card
                        className={classNames("p-3", "card-tab", { active: currentActiveTab === 2 })}
                        onClick={() => toggleTab(2)}
                    >
                        <img src={removeUnusedImg} alt="" />
                        <Badge
                            className="rounded-pill fs-5"
                            color={unusedIndexes.length !== 0 ? "primary" : "secondary"}
                        >
                            {unusedIndexes.length}
                        </Badge>
                        <h4 className="text-center">
                            Remove <br />
                            unused indexes
                        </h4>
                    </Card>
                </NavItem>
                <NavItem>
                    <Card
                        className={classNames("p-3", "card-tab", { active: currentActiveTab === 3 })}
                        onClick={() => toggleTab(3)}
                    >
                        <img src={unmergableIndexesImg} alt="" />
                        <Badge
                            className="rounded-pill fs-5"
                            color={unmergableIndexes.length !== 0 ? "primary" : "secondary"}
                        >
                            {unmergableIndexes.length}
                        </Badge>
                        <h4 className="text-center">
                            Unmergable
                            <br />
                            indexes
                        </h4>
                    </Card>
                </NavItem>
            </Nav>

            <Carousel
                activeIndex={currentActiveTab}
                className="carousel-auto-height mt-3 mb-4"
                style={{ height: carouselHeight }}
                next={() => console.log(carouselRefs.current[currentActiveTab].clientHeight)}
                previous={() => console.log("previous")}
            >
                <CarouselItem onExiting={setHeight} onExited={onCarouselExited}>
                    <div ref={(el) => (carouselRefs.current[0] = el)}>
                        <Card>
                            <Card className="bg-faded-primary p-4 d-block">
                                <div className="text-limit-width">
                                    <h2>Merge indexes</h2>
                                    Combining several indexes with similar purposes into a single index can reduce the
                                    number of times that data needs to be scanned.
                                    <br />
                                    Once a <strong>NEW</strong> merged index definition is created, the original indexes
                                    can be removed.
                                </div>
                            </Card>
                            <div className="p-2">
                                {mergableIndexes.length === 0 ? (
                                    <EmptySet>No indexes to merge</EmptySet>
                                ) : (
                                    <>
                                        <div className="mx-3">
                                            <Table className="mb-1 table-inner-border">
                                                <tbody>
                                                    <tr>
                                                        <td></td>
                                                        <td width={300}>
                                                            <div className="small-label">Last query time</div>
                                                        </td>
                                                        <td width={300}>
                                                            <div className="small-label">Last indexing time</div>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>

                                        {mergableIndexes.map((mergableGroup, groupKey) => (
                                            <RichPanel
                                                key={"mergeGroup-" + mergableGroup.mergedIndexDefinition.Name}
                                                hover
                                            >
                                                <RichPanelHeader className="px-3 py-2 flex-wrap flex-row gap-3">
                                                    <div className="mt-1">
                                                        <Button
                                                            color="primary"
                                                            size="sm"
                                                            className="rounded-pill"
                                                            onClick={() => navigateToMergeSuggestion(mergableGroup)}
                                                        >
                                                            <Icon icon="merge" />
                                                            Review suggested merge
                                                        </Button>
                                                    </div>
                                                    <div className="flex-grow-1">
                                                        <Table className="m-0 table-inner-border">
                                                            <tbody>
                                                                {mergableGroup.toMerge.map((index, indexKey) => (
                                                                    <tr key={"index-" + groupKey + "-" + indexKey}>
                                                                        <td>
                                                                            <div>
                                                                                <a
                                                                                    href={appUrl.forEditIndex(
                                                                                        index.name,
                                                                                        db
                                                                                    )}
                                                                                >
                                                                                    {index.name}{" "}
                                                                                    <Icon icon="newtab" margin="ms-1" />
                                                                                </a>
                                                                            </div>
                                                                        </td>

                                                                        <td width={300}>
                                                                            <div>{formatDate(index.lastQueryTime)}</div>
                                                                        </td>
                                                                        <td width={300}>
                                                                            <div>
                                                                                {formatDate(index.lastIndexingTime)}
                                                                            </div>
                                                                        </td>
                                                                    </tr>
                                                                ))}
                                                            </tbody>
                                                        </Table>
                                                    </div>
                                                </RichPanelHeader>
                                            </RichPanel>
                                        ))}
                                    </>
                                )}
                            </div>
                        </Card>
                    </div>
                </CarouselItem>
                <CarouselItem onExiting={setHeight} onExited={onCarouselExited}>
                    <div ref={(el) => (carouselRefs.current[1] = el)}>
                        <Card>
                            <Card className="bg-faded-primary p-4">
                                <div className="text-limit-width">
                                    <h2>Remove sub-indexes</h2>
                                    If an index is completely covered by another index (i.e., all its fields are present
                                    in the larger index) maintaining it does not provide any value and only adds
                                    unnecessary overhead. You can remove the subset index without losing any query
                                    optimization benefits.
                                </div>
                            </Card>
                            {surpassingIndexes.length === 0 ? (
                                <EmptySet>No subset indexes</EmptySet>
                            ) : (
                                <div className="p-2">
                                    <Button
                                        color="primary"
                                        className="mb-2 rounded-pill"
                                        onClick={deleteSelectedSurpassingIndexes}
                                        disabled={selectedSurpassingIndexes.length === 0}
                                    >
                                        Delete selected sub-indexes{" "}
                                        <Badge color="faded-primary" className="rounded-pill ms-1">
                                            {selectedSurpassingIndexes.length}
                                        </Badge>
                                    </Button>

                                    <RichPanel hover>
                                        <RichPanelHeader className="px-3 py-2 d-block">
                                            <Table responsive className="m-0 table-inner-border">
                                                <thead>
                                                    <tr>
                                                        <td>
                                                            <Checkbox
                                                                size="lg"
                                                                selected={surpassingSelectionState === "AllSelected"}
                                                                indeterminate={
                                                                    surpassingSelectionState === "SomeSelected"
                                                                }
                                                                toggleSelection={toggleAllSurpassingIndexes}
                                                            />
                                                        </td>
                                                        <td>
                                                            <div className="small-label">Sub-index</div>
                                                        </td>
                                                        <td width={50}></td>
                                                        <td>
                                                            <div className="small-label">Containing index</div>
                                                        </td>
                                                        <td>
                                                            <div className="small-label">
                                                                Last query time (sub-index)
                                                            </div>
                                                        </td>
                                                        <td>
                                                            <div className="small-label">
                                                                Last indexing time (sub-index)
                                                            </div>
                                                        </td>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {surpassingIndexes.map((index) => (
                                                        <tr key={"subindex-" + index.name}>
                                                            <td>
                                                                <Checkbox
                                                                    size="lg"
                                                                    selected={selectedSurpassingIndexes.includes(
                                                                        index.name
                                                                    )}
                                                                    toggleSelection={(x) =>
                                                                        toggleSurpassingIndex(
                                                                            x.currentTarget.checked,
                                                                            index.name
                                                                        )
                                                                    }
                                                                />
                                                            </td>
                                                            <td>
                                                                <div>
                                                                    <a href={appUrl.forEditIndex(index.name, db)}>
                                                                        {index.name}{" "}
                                                                        <Icon icon="newtab" margin="ms-1" />
                                                                    </a>
                                                                </div>
                                                            </td>
                                                            <td>
                                                                <div>⊇</div>
                                                            </td>
                                                            <td>
                                                                <div>
                                                                    <a href={appUrl.forEditIndex(index.name, db)}>
                                                                        {index.containingIndexName}{" "}
                                                                        <Icon icon="newtab" margin="ms-1" />
                                                                    </a>
                                                                </div>
                                                            </td>
                                                            <td width={300}>
                                                                <div>{formatDate(index.lastQueryingTime)}</div>
                                                            </td>
                                                            <td width={300}>
                                                                <div>{formatDate(index.lastIndexingTime)}</div>
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </Table>
                                        </RichPanelHeader>
                                    </RichPanel>
                                </div>
                            )}
                        </Card>
                    </div>
                </CarouselItem>
                <CarouselItem onExiting={setHeight} onExited={onCarouselExited}>
                    <div ref={(el) => (carouselRefs.current[2] = el)}>
                        <Card>
                            <Card className="bg-faded-primary p-4">
                                <div className="text-limit-width">
                                    <h2>Remove unused indexes</h2>
                                    Unused indexes still consume resources.
                                    <br />
                                    Indexes that have not been queried for over a week are listed below.
                                    <br />
                                    Review the list and consider deleting any unnecessary indexes.
                                </div>
                            </Card>
                            {unusedIndexes.length === 0 ? (
                                <EmptySet>No unused indexes</EmptySet>
                            ) : (
                                <div className="p-2">
                                    <Button
                                        color="primary"
                                        className="mb-2"
                                        onClick={deleteSelectedUnusedIndex}
                                        disabled={selectedUnusedIndexes.length === 0}
                                    >
                                        Delete selected indexes
                                        <Badge color="faded-primary" className="rounded-pill ms-1">
                                            {selectedUnusedIndexes.length}
                                        </Badge>
                                    </Button>
                                    <RichPanel hover>
                                        <RichPanelHeader className="px-3 py-2 d-block">
                                            <Table responsive className="m-0 table-inner-border">
                                                <thead>
                                                    <tr>
                                                        <td>
                                                            <Checkbox
                                                                size="lg"
                                                                selected={unusedSelectionState === "AllSelected"}
                                                                indeterminate={unusedSelectionState === "SomeSelected"}
                                                                toggleSelection={toggleAllUnusedIndexes}
                                                            />
                                                        </td>
                                                        <td>
                                                            <div className="small-label">Unused index</div>
                                                        </td>

                                                        <td>
                                                            <div className="small-label">Last query time</div>
                                                        </td>
                                                        <td>
                                                            <div className="small-label">Last indexing time</div>
                                                        </td>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {unusedIndexes.map((index) => (
                                                        <tr key={"unusedIndex-" + index.name}>
                                                            <td>
                                                                <Checkbox
                                                                    size="lg"
                                                                    selected={selectedUnusedIndexes.includes(
                                                                        index.name
                                                                    )}
                                                                    toggleSelection={(x) =>
                                                                        toggleUnusedIndex(
                                                                            x.currentTarget.checked,
                                                                            index.name
                                                                        )
                                                                    }
                                                                />
                                                            </td>
                                                            <td>
                                                                <div>
                                                                    <a href={appUrl.forEditIndex(index.name, db)}>
                                                                        {index.name}{" "}
                                                                        <Icon icon="newtab" margin="ms-1" />
                                                                    </a>
                                                                </div>
                                                            </td>
                                                            <td width={300}>
                                                                <div>{formatDate(index.lastQueryingTime)}</div>
                                                            </td>
                                                            <td width={300}>
                                                                <div>{formatDate(index.lastIndexingTime)}</div>
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </Table>
                                        </RichPanelHeader>
                                    </RichPanel>
                                </div>
                            )}
                        </Card>
                    </div>
                </CarouselItem>
                <CarouselItem onExiting={setHeight} onExited={onCarouselExited}>
                    <div ref={(el) => (carouselRefs.current[3] = el)}>
                        <Card>
                            <Card className="bg-faded-primary p-4">
                                <div className="text-limit-width">
                                    <h2>Unmergable indexes</h2>
                                    The following indexes cannot be merged. <br />
                                    See the specific reason explanation provided for each index.
                                </div>
                            </Card>

                            {unmergableIndexes.length === 0 ? (
                                <EmptySet>No unmergable indexes</EmptySet>
                            ) : (
                                <div className="p-2">
                                    <RichPanel hover>
                                        <RichPanelHeader className="px-3 py-2 d-block">
                                            <Table responsive className="m-0 table-inner-border">
                                                <thead>
                                                    <tr>
                                                        <td>
                                                            <div className="small-label">Index name</div>
                                                        </td>

                                                        <td>
                                                            <div className="small-label">Unmergable reason</div>
                                                        </td>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {unmergableIndexes.map((index, indexKey) => (
                                                        <tr key={"unmergable-" + indexKey}>
                                                            <td>
                                                                <div>
                                                                    <a href={appUrl.forEditIndex(index.name, db)}>
                                                                        {index.name}
                                                                        <Icon icon="newtab" margin="ms-1" />
                                                                    </a>
                                                                </div>
                                                            </td>
                                                            <td>
                                                                <div>{index.reason}</div>
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </Table>
                                        </RichPanelHeader>
                                    </RichPanel>
                                </div>
                            )}
                        </Card>
                    </div>
                </CarouselItem>
            </Carousel>
        </div>
    );
}

const formatDate = (date: Date) => {
    return (
        <>
            {moment.utc(date).local().fromNow()}{" "}
            <small className="text-muted">({moment.utc(date).format("MM/DD/YY, h:mma")})</small>
        </>
    );
};
