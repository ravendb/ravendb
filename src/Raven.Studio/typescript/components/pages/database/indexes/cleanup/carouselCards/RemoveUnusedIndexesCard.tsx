import appUrl from "common/appUrl";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Checkbox } from "components/common/Checkbox";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import {
    UseIndexCleanupResult,
    formatIndexCleanupDate,
} from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import { Card, Badge, Table } from "reactstrap";

interface RemoveUnusedIndexesCardProps {
    unused: UseIndexCleanupResult["unused"];
}

export default function RemoveUnusedIndexesCard({ unused }: RemoveUnusedIndexesCardProps) {
    return (
        <Card>
            <Card className="bg-faded-primary m-1 p-4">
                <div className="text-limit-width">
                    <h2>Remove unused indexes</h2>
                    Unused indexes still consume resources. Indexes that have not been queried for over a week are
                    listed below. Review the list and consider deleting any unnecessary indexes.
                </div>
            </Card>
            {unused.data.length === 0 ? <EmptySet>No unused indexes</EmptySet> : <MainPanel unused={unused} />}
        </Card>
    );
}

function MainPanel({ unused }: RemoveUnusedIndexesCardProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
        <div className="p-2">
            <ButtonWithSpinner
                color="primary"
                className="mb-2 rounded-pill"
                onClick={unused.deleteSelected}
                isSpinning={unused.isDeleting}
                disabled={unused.selected.length === 0}
            >
                <Icon icon="trash" />
                Delete selected indexes
                <Badge color="faded-primary" className="rounded-pill ms-1">
                    {unused.selected.length}
                </Badge>
            </ButtonWithSpinner>
            <RichPanel hover>
                <RichPanelHeader className="px-3 py-2 d-block">
                    <Table responsive className="m-0 table-inner-border">
                        <thead>
                            <tr>
                                <td>
                                    <Checkbox
                                        size="lg"
                                        color="primary"
                                        selected={unused.selectionState === "AllSelected"}
                                        indeterminate={unused.selectionState === "SomeSelected"}
                                        toggleSelection={unused.toggleAll}
                                    />
                                </td>
                                <td className="align-middle">
                                    <div className="small-label">Unused index</div>
                                </td>

                                <td className="align-middle">
                                    <div className="small-label">Last query time</div>
                                </td>
                                <td className="align-middle">
                                    <div className="small-label">Last indexing time</div>
                                </td>
                            </tr>
                        </thead>
                        <tbody>
                            {unused.data.map((index) => (
                                <tr key={"unusedIndex-" + index.name}>
                                    <td>
                                        <Checkbox
                                            size="lg"
                                            selected={unused.selected.includes(index.name)}
                                            toggleSelection={(x) => unused.toggle(x.currentTarget.checked, index.name)}
                                        />
                                    </td>
                                    <td>
                                        <div>
                                            <a href={appUrl.forEditIndex(index.name, databaseName)}>
                                                {index.name} <Icon icon="newtab" margin="ms-1" />
                                            </a>
                                        </div>
                                    </td>
                                    <td width={300}>
                                        <div>{formatIndexCleanupDate(index.lastQueryingTime)}</div>
                                    </td>
                                    <td width={300}>
                                        <div>{formatIndexCleanupDate(index.lastIndexingTime)}</div>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </Table>
                </RichPanelHeader>
            </RichPanel>
        </div>
    );
}
