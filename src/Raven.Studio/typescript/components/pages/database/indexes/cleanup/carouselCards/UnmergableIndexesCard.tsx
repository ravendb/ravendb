import appUrl from "common/appUrl";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { UseIndexCleanupResult } from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import { Card, Table } from "reactstrap";

interface UnmergableIndexesCardProps {
    unmergable: UseIndexCleanupResult["unmergable"];
}

export default function UnmergableIndexesCard({ unmergable }: UnmergableIndexesCardProps) {
    return (
        <Card>
            <Card className="bg-faded-primary m-1 p-4">
                <div className="text-limit-width">
                    <h2>Unmergeable indexes</h2>
                    The following indexes cannot be merged. See the specific reason explanation provided for each index.
                </div>
            </Card>

            {unmergable.data.length === 0 ? (
                <EmptySet>No unmergeable indexes</EmptySet>
            ) : (
                <MainPanel unmergable={unmergable} />
            )}
        </Card>
    );
}

function MainPanel({ unmergable }: UnmergableIndexesCardProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
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
                                    <div className="small-label">Unmergeable reason</div>
                                </td>
                            </tr>
                        </thead>
                        <tbody>
                            {unmergable.data.map((index, indexKey) => (
                                <tr key={"unmergable-" + indexKey}>
                                    <td>
                                        <div>
                                            <a href={appUrl.forEditIndex(index.name, databaseName)}>
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
    );
}
