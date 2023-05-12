import React, { useEffect, useLayoutEffect, useRef, useState } from "react";

import { Icon } from "components/common/Icon";
import {
    Alert,
    Badge,
    Button,
    Card,
    Carousel,
    CarouselControl,
    CarouselItem,
    Collapse,
    Nav,
    NavItem,
    NavLink,
    TabContent,
    TabPane,
    Table,
} from "reactstrap";
import { RichPanel, RichPanelDetails, RichPanelHeader } from "components/common/RichPanel";
import { Checkbox } from "components/common/Checkbox";

import moment from "moment";
import { EmptySet } from "components/common/EmptySet";

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

const formatDate = (date: Date) => {
    return (
        <>
            {moment.utc(date).local().fromNow()}{" "}
            <small className="text-muted">({moment.utc(date).format("MM/DD/YY, h:mma")})</small>
        </>
    );
};

interface IndexCleanupProps {
    mergableIndexes: IndexInfo[][];
    subIndexes: IndexInfo[];
    unusedIndexes: IndexInfo[];
    unmergableIndexes: UnmergableIndexInfo[];
}

export function IndexCleanup(props: IndexCleanupProps) {
    const { mergableIndexes, subIndexes, unusedIndexes, unmergableIndexes } = props;

    const [currentActiveTab, setCurrentActiveTab] = useState(0);
    const [carouselHeight, setCarouselHeight] = useState(null);
    const carouselRefs = useRef([]);

    const toggleTab = (tab: number) => {
        if (currentActiveTab !== tab) {
            setHeight();
            setCurrentActiveTab(tab);
        }
    };

    const setHeight = () => {
        const activeCarouselItem = carouselRefs.current[currentActiveTab];
        if (activeCarouselItem) {
            setCarouselHeight(activeCarouselItem.clientHeight);
        }
    };

    const onCarouselExiting = () => {
        setHeight();
    };

    const onCarouselExited = () => {
        setCarouselHeight(null);
    };

    return (
        <>
            <h2 className="mb-4">
                <Icon icon="clean" /> Index Cleanup
            </h2>
            <div className="text-limit-width">
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

            <Nav className="d-flex gap-4">
                <NavItem>
                    <Card className="active p-3" onClick={() => toggleTab(0)}>
                        <h2>
                            Merge
                            <br />
                            indexes
                            <small className="text-muted ms-2">
                                <Badge className="rounded-pill">{mergableIndexes.length}</Badge>
                            </small>
                        </h2>
                    </Card>
                </NavItem>
                <NavItem>
                    <Card className="p-3" onClick={() => toggleTab(1)}>
                        <h2>
                            Remove
                            <br />
                            sub-indexes
                            <small>
                                <Badge className="rounded-pill ms-2">{subIndexes.length}</Badge>
                            </small>
                        </h2>
                    </Card>
                </NavItem>
                <NavItem>
                    <Card className="p-3" onClick={() => toggleTab(2)}>
                        <h2>
                            Remove <br />
                            unused indexes{" "}
                            <small>
                                <Badge className="rounded-pill ms-2">{unusedIndexes.length}</Badge>
                            </small>
                        </h2>
                    </Card>
                </NavItem>
                <NavItem>
                    <Card className="p-3" onClick={() => toggleTab(3)}>
                        <h2>
                            Unmergable
                            <br />
                            indexes{" "}
                            <small>
                                <Badge className="rounded-pill ms-2">{unmergableIndexes.length}</Badge>
                            </small>
                        </h2>
                    </Card>
                </NavItem>
            </Nav>

            <Carousel
                activeIndex={currentActiveTab}
                enableTouch={false}
                interval={null}
                keyboard={false}
                ride="carousel"
                className="carousel-auto-height my-4"
                style={{ height: carouselHeight }}
                next={() => console.log(carouselRefs.current[currentActiveTab].clientHeight)}
                previous={() => console.log("previous")}
            >
                <CarouselItem onExiting={onCarouselExiting} onExited={onCarouselExited} key={"carousel-1"}>
                    <div ref={(el) => (carouselRefs.current[0] = el)}>
                        <RichPanel>
                            <Card className="bg-faded-primary p-4 d-block">
                                Combining several indexes with similar purposes into a single index can reduce the
                                number of times that data needs to be scanned.
                                <br />
                                Indexes will merged into a <strong>NEW</strong> index definition. The original indexes
                                can then be removed.
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
                        </RichPanel>
                    </div>
                </CarouselItem>
                <CarouselItem onExiting={onCarouselExiting} onExited={onCarouselExited} key={"carousel-2"}>
                    <div ref={(el) => (carouselRefs.current[1] = el)} className="p-4">
                        <Alert color="info">
                            Indexes with index-fields that are a subset of other indexes. Please review index usage
                            before deleting selected items.
                        </Alert>
                        {subIndexes.length === 0 ? (
                            <EmptySet>No subset indexes</EmptySet>
                        ) : (
                            <>
                                <Button color="primary" className="mb-2">
                                    Delete selected sub-indexes{" "}
                                    <Badge color="faded-primary" className="rounded-pill ms-1">
                                        2
                                    </Badge>
                                </Button>

                                <RichPanel hover>
                                    <RichPanelHeader className="px-3 py-2">
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
                                                        <div className="small-label">Last query time (sub-index)</div>
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
                            </>
                        )}
                    </div>
                </CarouselItem>
                <CarouselItem onExiting={onCarouselExiting} onExited={onCarouselExited} key={"carousel-3"}>
                    <div ref={(el) => (carouselRefs.current[2] = el)} className="p-4">
                        <Alert color="info">
                            Indexes that were not queried for over a week. Please review index usage before deleting
                            selected items.
                        </Alert>
                        {unusedIndexes.length === 0 ? (
                            <EmptySet>No unused indexes</EmptySet>
                        ) : (
                            <>
                                <Button color="primary" className="mb-2">
                                    Delete selected indexes
                                    <Badge color="faded-primary" className="rounded-pill ms-1">
                                        2
                                    </Badge>
                                </Button>
                                <RichPanel hover>
                                    <RichPanelHeader className="px-3 py-2">
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
                            </>
                        )}
                    </div>
                </CarouselItem>
                <CarouselItem onExiting={onCarouselExiting} onExited={onCarouselExited} key={"carousel-4"}>
                    <div ref={(el) => (carouselRefs.current[3] = el)} className="p-4">
                        {unmergableIndexes.length === 0 ? (
                            <EmptySet>No unmergable indexes</EmptySet>
                        ) : (
                            <RichPanel hover>
                                <RichPanelHeader className="px-3 py-2">
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
                        )}
                    </div>
                </CarouselItem>
            </Carousel>
            <h1>*** Height check ***</h1>
        </>
    );
}
