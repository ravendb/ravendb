import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import React from "react";
import genUtils from "common/generalUtils";
import changeVectorUtils from "common/changeVectorUtils";
import { Card, Table, UncontrolledTooltip } from "reactstrap";
import { LazyLoad } from "components/common/LazyLoad";
import { useAppSelector } from "components/store";
import { selectAllDatabaseDetails } from "components/pages/database/status/statistics/logic/statisticsSlice";

interface DetailsBlockProps {
    children: (data: DetailedDatabaseStatistics, location: databaseLocationSpecifier) => JSX.Element;
}

export function DetailedDatabaseStats() {
    const perNodeStats = useAppSelector(selectAllDatabaseDetails);

    function DetailsBlock(props: DetailsBlockProps): JSX.Element {
        const { children } = props;

        return (
            <>
                {perNodeStats.map((perNode) => {
                    const { location, data: stat, status } = perNode;

                    if (status === "failure") {
                        return (
                            <td key={genUtils.formatLocation(location)} className="text-danger">
                                <i className="icon-cancel" title="Load error" />
                            </td>
                        );
                    }

                    return (
                        <td key={genUtils.formatLocation(location)}>
                            {status === "success" || (status === "loading" && stat) ? (
                                children(stat, location)
                            ) : (
                                <LazyLoad active>
                                    <div>Loading...</div>
                                </LazyLoad>
                            )}
                        </td>
                    );
                })}
            </>
        );
    }

    return (
        <section className="mt-6">
            <h2 className="on-base-background">Detailed Database Stats</h2>
            <Card className="panel mt-4">
                <Table responsive bordered striped>
                    <thead>
                        <tr>
                            <th>&nbsp;</th>
                            {perNodeStats.map(({ location }) => {
                                return (
                                    <th key={genUtils.formatLocation(location)}>{genUtils.formatLocation(location)}</th>
                                );
                            })}
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>
                                <i className="icon-database-id"></i> <span>Database ID</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.DatabaseId}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-vector"></i> <span>Database Change Vector</span>
                            </td>
                            <DetailsBlock>
                                {(data, location) => {
                                    const id = "js-cv-" + location.nodeTag + "-" + location.shardNumber;

                                    const formattedChangeVector = changeVectorUtils.formatChangeVector(
                                        data.DatabaseChangeVector,
                                        changeVectorUtils.shouldUseLongFormat([data.DatabaseChangeVector])
                                    );

                                    if (formattedChangeVector.length === 0) {
                                        return <span>not yet defined</span>;
                                    }

                                    return (
                                        <div id={id}>
                                            {formattedChangeVector.map((cv) => (
                                                <div key={cv.fullFormat} className="badge bg-secondary margin-right-xs">
                                                    {cv.shortFormat}
                                                </div>
                                            ))}
                                            <UncontrolledTooltip target={id}>
                                                <div>
                                                    {formattedChangeVector.map((cv) => (
                                                        <small key={cv.fullFormat}>{cv.fullFormat}</small>
                                                    ))}
                                                </div>
                                            </UncontrolledTooltip>
                                        </div>
                                    );
                                }}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-storage"></i>
                                <span>Size On Disk</span>
                            </td>
                            <DetailsBlock>
                                {(data, location) => {
                                    const id = "js-size-on-disk-" + location.nodeTag + "-" + location.shardNumber;
                                    return (
                                        <span id={id}>
                                            {genUtils.formatBytesToSize(
                                                data.SizeOnDisk.SizeInBytes + data.TempBuffersSizeOnDisk.SizeInBytes
                                            )}
                                            <UncontrolledTooltip target={id}>
                                                <div>
                                                    Data:{" "}
                                                    <strong>
                                                        {genUtils.formatBytesToSize(data.SizeOnDisk.SizeInBytes)}
                                                    </strong>
                                                    <br />
                                                    Temp:{" "}
                                                    <strong>
                                                        {genUtils.formatBytesToSize(
                                                            data.TempBuffersSizeOnDisk.SizeInBytes
                                                        )}
                                                    </strong>
                                                    <br />
                                                    Total:{" "}
                                                    <strong>
                                                        {genUtils.formatBytesToSize(
                                                            data.SizeOnDisk.SizeInBytes +
                                                                data.TempBuffersSizeOnDisk.SizeInBytes
                                                        )}
                                                    </strong>
                                                </div>
                                            </UncontrolledTooltip>
                                        </span>
                                    );
                                }}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-etag"></i>
                                <span>Last Document ETag</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.LastDocEtag}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-etag"></i>
                                <span>Last Database ETag</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.LastDatabaseEtag}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-server"></i>
                                <span>Architecture</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.Is64Bit ? "64 Bit" : "32 Bit"}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-documents"></i>
                                <span>Documents </span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfDocuments.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-new-counter"></i>
                                <span>Counters</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfCounterEntries.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-identities"></i>
                                <span>Identities</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfIdentities.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-indexing"></i>
                                <span>Indexes</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfIndexes.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-revisions"></i>
                                <span>Revisions</span>
                            </td>
                            <DetailsBlock>
                                {(data) => <>{(data.CountOfRevisionDocuments ?? 0).toLocaleString()}</>}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-conflicts"></i>
                                <span>Conflicts</span>
                            </td>
                            <DetailsBlock>
                                {(data) => <>{data.CountOfDocumentsConflicts.toLocaleString()}</>}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-attachment"></i>
                                <span>Attachments</span>
                            </td>
                            <DetailsBlock>
                                {(data) => (
                                    <div>
                                        <span>{data.CountOfAttachments.toLocaleString()}</span>
                                        {data.CountOfAttachments !== data.CountOfUniqueAttachments && (
                                            <>
                                                <span className="text-muted">/</span>
                                                <small>
                                                    <span className="text-muted">
                                                        {data.CountOfUniqueAttachments.toLocaleString()} unique
                                                    </span>
                                                </small>
                                            </>
                                        )}
                                    </div>
                                )}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-cmp-xchg"></i>
                                <span>Compare Exchange</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfCompareExchange.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-zombie"></i>
                                <span>Tombstones</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfTombstones.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <i className="icon-timeseries-settings"></i>
                                <span>Time Series Segments</span>
                            </td>
                            <DetailsBlock>
                                {(data) => <>{data.CountOfTimeSeriesSegments.toLocaleString()}</>}
                            </DetailsBlock>
                        </tr>
                    </tbody>
                </Table>
            </Card>
        </section>
    );
}
