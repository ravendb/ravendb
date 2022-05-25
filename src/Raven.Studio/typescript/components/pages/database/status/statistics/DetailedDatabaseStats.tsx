import database from "models/resources/database";
import { useCallback, useEffect, useState } from "react";
import { useServices } from "../../../../hooks/useServices";
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import { locationAwareLoadableData } from "../../../../models/common";
import React from "react";
import { produce } from "immer";
import { databaseLocationComparator } from "../../../../utils/common";
import genUtils from "common/generalUtils";
import changeVectorUtils from "common/changeVectorUtils";
import { UncontrolledTooltip } from "../../../../common/UncontrolledTooltip";

interface DetailedDatabaseStatsProps {
    database: database;
}

function initState(db: database): locationAwareLoadableData<DetailedDatabaseStatistics>[] {
    return db.getLocations().map((location) => {
        return {
            data: null,
            location,
            error: null,
            status: "loading",
        };
    });
}
interface DetailsBlockProps {
    children: (data: DetailedDatabaseStatistics, location: databaseLocationSpecifier) => JSX.Element;
}

export function DetailedDatabaseStats(props: DetailedDatabaseStatsProps) {
    const { database } = props;

    const { databasesService } = useServices();

    const [perNodeStats, setPerNodeStats] = useState<locationAwareLoadableData<DetailedDatabaseStatistics>[]>(
        initState(database)
    );

    function DetailsBlock(props: DetailsBlockProps): JSX.Element {
        const { children } = props;

        return (
            <>
                {database.getLocations().map((location) => {
                    const stat = perNodeStats.find((x) => databaseLocationComparator(x.location, location));

                    return (
                        <td key={genUtils.formatLocation(location)}>
                            {stat.status === "loaded" ? children(stat.data, location) : "loading..."}
                        </td>
                    );
                })}
            </>
        );
    }

    const loadDetailedStats = useCallback(() => {
        const locations = database.getLocations();

        locations.forEach(async (location) => {
            try {
                const stats = await databasesService.getDetailedStats(database, location);
                setPerNodeStats(
                    produce((draft) => {
                        const itemToUpdate = draft.find((x) => databaseLocationComparator(x.location, location));
                        itemToUpdate.error = null;
                        itemToUpdate.status = "loaded";
                        itemToUpdate.data = stats;
                    })
                );
            } catch (e) {
                setPerNodeStats(
                    produce((draft) => {
                        const itemToUpdate = draft.find((x) => databaseLocationComparator(x.location, location));
                        itemToUpdate.error = e;
                        itemToUpdate.status = "error";
                    })
                );
            }
        });
    }, [database, databasesService]);

    useEffect(() => {
        loadDetailedStats();
    }, [database, databasesService]);

    return (
        <div className="margin-top">
            <h2>Detailed Database Stats</h2>

            <table className="table table-bordered table-striped">
                <thead>
                    <tr>
                        <th>&nbsp;</th>
                        {database.getLocations().map((location) => {
                            return <th key={genUtils.formatLocation(location)}>{genUtils.formatLocation(location)}</th>;
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
                                            <div key={cv.fullFormat} className="badge badge-default margin-right-xs">
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
                                                    {genUtils.formatBytesToSize(data.TempBuffersSizeOnDisk.SizeInBytes)}
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
                            <span>Documents Count</span>
                        </td>
                        <DetailsBlock>{(data) => <>{data.CountOfDocuments.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    <tr>
                        <td>
                            <i className="icon-new-counter"></i>
                            <span>Counters Count</span>
                        </td>
                        <DetailsBlock>{(data) => <>{data.CountOfCounterEntries.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    <tr>
                        <td>
                            <i className="icon-identities"></i>
                            <span>Identities Count</span>
                        </td>
                        <DetailsBlock>{(data) => <>{data.CountOfIdentities.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    <tr>
                        <td>
                            <i className="icon-indexing"></i>
                            <span>Indexes Count</span>
                        </td>
                        <DetailsBlock>{(data) => <>{data.CountOfIndexes.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    <tr>
                        <td>
                            <i className="icon-revisions"></i>
                            <span>Revisions Count</span>
                        </td>
                        <DetailsBlock>
                            {(data) => <>{(data.CountOfRevisionDocuments ?? 0).toLocaleString()}</>}
                        </DetailsBlock>
                    </tr>
                    <tr>
                        <td>
                            <i className="icon-conflicts"></i>
                            <span>Conflicts Count</span>
                        </td>
                        <DetailsBlock>{(data) => <>{data.CountOfDocumentsConflicts.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    <tr>
                        <td>
                            <i className="icon-attachment"></i>
                            <span>Attachments Count</span>
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
                            <span>Compare Exchange Count</span>
                        </td>
                        <DetailsBlock>{(data) => <>{data.CountOfCompareExchange.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    <tr>
                        <td>
                            <i className="icon-zombie"></i>
                            <span>Tombstones Count</span>
                        </td>
                        <DetailsBlock>{(data) => <>{data.CountOfTombstones.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    <tr>
                        <td>
                            <i className="icon-timeseries-settings"></i>
                            <span>Time Series Segments Count</span>
                        </td>
                        <DetailsBlock>{(data) => <>{data.CountOfTimeSeriesSegments.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                </tbody>
            </table>
        </div>
    );
}
