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
import { Card, Table, Badge } from "reactstrap";

interface RemoveSubindexesCardProps {
    surpassing: UseIndexCleanupResult["surpassing"];
}

export default function RemoveSubindexesCard({ surpassing }: RemoveSubindexesCardProps) {
    return (
        <Card>
            <Card className="bg-faded-primary m-1 p-4">
                <div className="text-limit-width">
                    <h2>Remove sub-indexes</h2>
                    If an index is completely covered by another index (i.e., all its fields are present in the larger
                    index) maintaining it does not provide any value and only adds unnecessary overhead. You can remove
                    the subset index without losing any query optimization benefits.
                </div>
            </Card>
            {surpassing.data.length === 0 ? (
                <EmptySet>No subset indexes</EmptySet>
            ) : (
                <MainPanel surpassing={surpassing} />
            )}
        </Card>
    );
}

function MainPanel({ surpassing }: RemoveSubindexesCardProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
        <div className="p-2">
            <ButtonWithSpinner
                color="primary"
                className="mb-2 rounded-pill"
                onClick={surpassing.deleteSelected}
                isSpinning={surpassing.isDeleting}
                disabled={surpassing.selected.length === 0}
            >
                <Icon icon="trash" />
                Delete selected sub-indexes{" "}
                <Badge color="faded-primary" className="rounded-pill ms-1">
                    {surpassing.selected.length}
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
                                        selected={surpassing.selectionState === "AllSelected"}
                                        indeterminate={surpassing.selectionState === "SomeSelected"}
                                        toggleSelection={surpassing.toggleAll}
                                    />
                                </td>
                                <td className="align-middle">
                                    <div className="small-label">Sub-index</div>
                                </td>
                                <td width={50}></td>
                                <td className="align-middle">
                                    <div className="small-label">Containing index</div>
                                </td>
                                <td className="align-middle">
                                    <div className="small-label">Last query time (sub-index)</div>
                                </td>
                                <td className="align-middle">
                                    <div className="small-label">Last indexing time (sub-index)</div>
                                </td>
                            </tr>
                        </thead>
                        <tbody>
                            {surpassing.data.map((index) => (
                                <tr key={"subindex-" + index.name}>
                                    <td>
                                        <Checkbox
                                            size="lg"
                                            selected={surpassing.selected.includes(index.name)}
                                            toggleSelection={(x) =>
                                                surpassing.toggle(x.currentTarget.checked, index.name)
                                            }
                                        />
                                    </td>
                                    <td>
                                        <div>
                                            <a href={appUrl.forEditIndex(index.name, databaseName)}>
                                                {index.name} <Icon icon="newtab" margin="ms-1" />
                                            </a>
                                        </div>
                                    </td>
                                    <td>
                                        <div>⊆</div>
                                    </td>
                                    <td>
                                        <div>
                                            <a href={appUrl.forEditIndex(index.containingIndexName, databaseName)}>
                                                {index.containingIndexName} <Icon icon="newtab" margin="ms-1" />
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
