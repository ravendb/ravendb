import appUrl from "common/appUrl";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import {
    UseIndexCleanupResult,
    formatIndexCleanupDate,
} from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import { Card, Table, Button } from "reactstrap";

interface MergeIndexesCardProps {
    mergable: UseIndexCleanupResult["mergable"];
}

export default function MergeIndexesCard({ mergable }: MergeIndexesCardProps) {
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    return (
        <Card>
            <Card className="bg-faded-primary p-4 m-1 d-block">
                <div className="text-limit-width">
                    <h2>Merge indexes</h2>
                    Combining several indexes with similar purposes into a single index can reduce the number of times
                    that data needs to be scanned.
                    <br />
                    Once a <strong>NEW</strong> merged index definition is created, the original indexes can be removed.
                </div>
            </Card>
            {hasIndexCleanup && (
                <div className="p-2">
                    {mergable.data.length === 0 ? (
                        <EmptySet>No indexes to merge</EmptySet>
                    ) : (
                        <MainPanel mergable={mergable} />
                    )}
                </div>
            )}
        </Card>
    );
}

function MainPanel({ mergable }: MergeIndexesCardProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
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

            {mergable.data.map((mergableGroup, groupKey) => (
                <RichPanel key={"mergeGroup-" + groupKey} hover>
                    <RichPanelHeader className="px-3 py-2 flex-wrap flex-row gap-3">
                        <div className="mt-1">
                            <Button
                                color="primary"
                                size="sm"
                                className="rounded-pill"
                                onClick={() => mergable.navigateToMergeSuggestion(mergableGroup)}
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
                                                    <a href={appUrl.forEditIndex(index.name, databaseName)}>
                                                        {index.name} <Icon icon="newtab" margin="ms-1" />
                                                    </a>
                                                </div>
                                            </td>

                                            <td width={300}>
                                                <div>{formatIndexCleanupDate(index.lastQueryTime)}</div>
                                            </td>
                                            <td width={300}>
                                                <div>{formatIndexCleanupDate(index.lastIndexingTime)}</div>
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
    );
}
