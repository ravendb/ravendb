import React, { useState } from "react";

import { Icon } from "components/common/Icon";
import { Alert, Badge, Button, Collapse, Table } from "reactstrap";
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
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
    const [collapseMerges, setCollapseMerges] = useState(false);

    const toggleCollapseMerges = () => {
        setCollapseMerges(!collapseMerges);
    };

    const [collapseSurpassing, setCollapseSurpassing] = useState(false);
    const toggleCollapseSurpassing = () => {
        setCollapseSurpassing(!collapseSurpassing);
    };
    const [collapseUnused, setCollapseUnused] = useState(false);
    const toggleCollapseUnused = () => {
        setCollapseUnused(!collapseUnused);
    };
    const [collapseUnmergable, setCollapseUnmergable] = useState(false);
    const toggleCollapseUnmergable = () => {
        setCollapseUnmergable(!collapseUnmergable);
    };
    return (
        <>
            <h2 className="mb-4">
                <Icon icon="clean" /> Index Cleanup
            </h2>
            <h3 onClick={toggleCollapseMerges} className="cursor-pointer">
                <Icon icon={collapseMerges ? "expand" : "collapse"} color="primary" />
                Merge indexes
                <small className="text-muted ms-2">
                    <Badge className="rounded-pill">{mergableIndexes.length}</Badge>
                </small>
            </h3>

            <Collapse isOpen={!collapseMerges}>
                <Alert color="info">
                    Indexes that can be merged into a NEW index definition. The original indexes can then be removed.
                </Alert>

                {mergableIndexes.length === 0 ? (
                    <EmptySet>No indexes to merge</EmptySet>
                ) : (
                    <div className="pb-5">
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
                                        <Button color="primary" size="sm">
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
                    </div>
                )}
            </Collapse>

            <h3 onClick={toggleCollapseSurpassing} className="cursor-pointer">
                <Icon icon={collapseSurpassing ? "expand" : "collapse"} color="primary" />
                Remove sub-indexes
                <small>
                    <Badge className="rounded-pill ms-2">{subIndexes.length}</Badge>
                </small>
            </h3>

            <Collapse isOpen={!collapseSurpassing}>
                <Alert color="info">
                    Indexes with index-fields that are a subset of other indexes. Please review index usage before
                    deleting selected items.
                </Alert>
                {subIndexes.length === 0 ? (
                    <EmptySet>No subset indexes</EmptySet>
                ) : (
                    <div className="pb-5">
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
                                                <div className="small-label">Last indexing time (sub-index)</div>
                                            </td>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {subIndexes.map((index, indexKey) => (
                                            <tr key={"subindex-" + indexKey}>
                                                <td>
                                                    <Checkbox size="lg" selected={null} toggleSelection={null} />
                                                </td>
                                                <td>
                                                    <div>
                                                        <a href="#">
                                                            {index.indexName} <Icon icon="newtab" margin="ms-1" />
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
            </Collapse>

            <h3 onClick={toggleCollapseUnused} className="cursor-pointer">
                <Icon icon={collapseUnused ? "expand" : "collapse"} color="primary" />
                Remove unused indexes
                <small>
                    <Badge className="rounded-pill ms-2">{unusedIndexes.length}</Badge>
                </small>
            </h3>

            <Collapse isOpen={!collapseUnused}>
                <Alert color="info">
                    Indexes that were not queried for over a week. Please review index usage before deleting selected
                    items.
                </Alert>
                {unusedIndexes.length === 0 ? (
                    <EmptySet>No unused indexes</EmptySet>
                ) : (
                    <div className="pb-5">
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
                                                    <Checkbox size="lg" selected={null} toggleSelection={null} />
                                                </td>
                                                <td>
                                                    <div>
                                                        <a href="#">
                                                            {index.indexName} <Icon icon="newtab" margin="ms-1" />
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
            </Collapse>

            <h3 onClick={toggleCollapseUnmergable} className="cursor-pointer">
                <Icon icon={collapseUnmergable ? "expand" : "collapse"} color="primary" />
                Unmergable indexes
                <small>
                    <Badge className="rounded-pill ms-2">{unmergableIndexes.length}</Badge>
                </small>
            </h3>

            <Collapse isOpen={!collapseUnmergable}>
                {unmergableIndexes.length === 0 ? (
                    <EmptySet>No unmergable indexes</EmptySet>
                ) : (
                    <div className="pb-5">
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
                    </div>
                )}
            </Collapse>
        </>
    );
}
