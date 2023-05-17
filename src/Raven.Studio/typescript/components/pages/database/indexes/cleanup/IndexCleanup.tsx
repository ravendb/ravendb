import React, { useRef, useState } from "react";
import { Icon } from "components/common/Icon";
import { Badge, Button, Card, Carousel, CarouselItem, Nav, NavItem, Table } from "reactstrap";
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
import { Checkbox } from "components/common/Checkbox";
import moment from "moment";
import { EmptySet } from "components/common/EmptySet";
import classNames from "classnames";

const mergeIndexesImg = require("Content/img/pages/indexCleanup/merge-indexes.svg");
const removeSubindexesImg = require("Content/img/pages/indexCleanup/remove-subindexes.svg");
const removeUnusedImg = require("Content/img/pages/indexCleanup/remove-unused.svg");
const unmergableIndexesImg = require("Content/img/pages/indexCleanup/unmergable-indexes.svg");

export interface IndexInfo {
    indexName: string;
    containingIndexName?: string;
    lastQuery: Date;
    lastIndexing: Date;
}

export interface UnmergableIndexInfo {
    indexName: string;
    unmergableReason: string;
}

interface IndexCleanupProps {
    mergableIndexes: IndexInfo[][];
    subIndexes: IndexInfo[];
    unusedIndexes: IndexInfo[];
    unmergableIndexes: UnmergableIndexInfo[];
}

export function IndexCleanup(props: IndexCleanupProps) {
    const { mergableIndexes, subIndexes, unusedIndexes, unmergableIndexes } = props;

    function activeNonEmpty() {
        if (mergableIndexes.length !== 0) return 0;
        if (subIndexes.length !== 0) return 1;
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
                        <Badge className="rounded-pill fs-5" color={subIndexes.length !== 0 ? "primary" : "secondary"}>
                            {subIndexes.length}
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
                enableTouch={false}
                interval={null}
                keyboard={false}
                ride="carousel"
                className="carousel-auto-height mt-3 mb-4"
                style={{ height: carouselHeight }}
                next={() => console.log(carouselRefs.current[currentActiveTab].clientHeight)}
                previous={() => console.log("previous")}
            >
                <CarouselItem onExiting={setHeight} onExited={onCarouselExited} key={"carousel-1"}>
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
                                            <RichPanel key={"mergeGroup-" + groupKey} hover>
                                                <RichPanelHeader className="px-3 py-2 flex-wrap flex-row gap-3">
                                                    <div className="mt-1">
                                                        <Button color="primary" size="sm" className="rounded-pill">
                                                            <Icon icon="merge" />
                                                            Review suggested merge
                                                        </Button>
                                                    </div>
                                                    <div className="flex-grow-1">
                                                        <Table className="m-0 table-inner-border">
                                                            <tbody>
                                                                {mergableGroup.map((index, indexKey) => (
                                                                    <tr key={"index-" + groupKey + "-" + indexKey}>
                                                                        <td>
                                                                            <div>
                                                                                <a href="#">
                                                                                    {index.indexName}{" "}
                                                                                    <Icon icon="newtab" margin="ms-1" />
                                                                                </a>
                                                                            </div>
                                                                        </td>

                                                                        <td width={300}>
                                                                            <div>{formatDate(index.lastQuery)}</div>
                                                                        </td>
                                                                        <td width={300}>
                                                                            <div>{formatDate(index.lastIndexing)}</div>
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
                <CarouselItem onExiting={setHeight} onExited={onCarouselExited} key={"carousel-2"}>
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
                            {subIndexes.length === 0 ? (
                                <EmptySet>No subset indexes</EmptySet>
                            ) : (
                                <div className="p-2">
                                    <Button color="primary" className="mb-2 rounded-pill">
                                        Delete selected sub-indexes{" "}
                                        <Badge color="faded-primary" className="rounded-pill ms-1">
                                            2
                                        </Badge>
                                    </Button>

                                    <RichPanel hover>
                                        <RichPanelHeader className="px-3 py-2 d-block">
                                            <Table responsive className="m-0 table-inner-border">
                                                <thead>
                                                    <tr>
                                                        <td width={50}></td>
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
                                                    {subIndexes.map((index, indexKey) => (
                                                        <tr key={"subindex-" + indexKey}>
                                                            <td>
                                                                <Checkbox
                                                                    size="lg"
                                                                    selected={null}
                                                                    toggleSelection={null}
                                                                />
                                                            </td>
                                                            <td>
                                                                <div>
                                                                    <a href="#">
                                                                        {index.indexName}{" "}
                                                                        <Icon icon="newtab" margin="ms-1" />
                                                                    </a>
                                                                </div>
                                                            </td>
                                                            <td>
                                                                <div>⊇</div>
                                                            </td>
                                                            <td>
                                                                <div>
                                                                    <a href="#">
                                                                        {index.containingIndexName}{" "}
                                                                        <Icon icon="newtab" margin="ms-1" />
                                                                    </a>
                                                                </div>
                                                            </td>
                                                            <td width={300}>
                                                                <div>{formatDate(index.lastQuery)}</div>
                                                            </td>
                                                            <td width={300}>
                                                                <div>{formatDate(index.lastIndexing)}</div>
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
                <CarouselItem onExiting={setHeight} onExited={onCarouselExited} key={"carousel-3"}>
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
                                    <Button color="primary" className="mb-2">
                                        Delete selected indexes
                                        <Badge color="faded-primary" className="rounded-pill ms-1">
                                            2
                                        </Badge>
                                    </Button>
                                    <RichPanel hover>
                                        <RichPanelHeader className="px-3 py-2 d-block">
                                            <Table responsive className="m-0 table-inner-border">
                                                <thead>
                                                    <tr>
                                                        <td width={50}></td>
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
                                                    {unusedIndexes.map((index, indexKey) => (
                                                        <tr key={"unusedIndex-" + indexKey}>
                                                            <td>
                                                                <Checkbox
                                                                    size="lg"
                                                                    selected={null}
                                                                    toggleSelection={null}
                                                                />
                                                            </td>
                                                            <td>
                                                                <div>
                                                                    <a href="#">
                                                                        {index.indexName}{" "}
                                                                        <Icon icon="newtab" margin="ms-1" />
                                                                    </a>
                                                                </div>
                                                            </td>
                                                            <td width={300}>
                                                                <div>{formatDate(index.lastQuery)}</div>
                                                            </td>
                                                            <td width={300}>
                                                                <div>{formatDate(index.lastIndexing)}</div>
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
                <CarouselItem onExiting={setHeight} onExited={onCarouselExited} key={"carousel-4"}>
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
                                                                    <a href="#">
                                                                        {index.indexName}
                                                                        <Icon icon="newtab" margin="ms-1" />
                                                                    </a>
                                                                </div>
                                                            </td>
                                                            <td>
                                                                <div>{index.unmergableReason}</div>
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
