import React, { memo } from "react";
import Badge from "react-bootstrap/Badge";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import StatTile from "./StatTile";
import genUtils from "common/generalUtils";
import { formatNumber } from "./analyzerUtils";

type DatabaseStatistics = Raven.Client.Documents.Operations.DatabaseStatistics;

interface DatabaseStatsProps {
    packageId: string;
    database: string;
    node: string;
}

// On-demand per-node database statistics from the analyzer databases/stats endpoint - the full
// DatabaseStatistics (counts, sizes, change vector, stale indexes) beyond the per-node overview.
export default memo(function DatabaseStats({ packageId, database, node }: DatabaseStatsProps) {
    const { manageServerService } = useServices();

    const stats = useAsync(async () => {
        if (!node) {
            return null as DatabaseStatistics | null;
        }
        return manageServerService.getDebugPackageDatabaseStats(packageId, node, database);
    }, [packageId, node, database]);

    const info = stats.result;

    return (
        <div className="database-stats">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="m-0">Statistics</h3>
                    {stats.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> Loading statistics for node {node}...
                        </div>
                    ) : stats.error ? (
                        <RichAlert variant="danger">
                            Could not load statistics for node {node}. The package may not contain stats for this
                            database, or the report expired.
                        </RichAlert>
                    ) : !info ? (
                        <EmptySet compact className="justify-content-center">
                            No statistics for {database} on node {node}
                        </EmptySet>
                    ) : (
                        <>
                            <div className="overview-stats gap-2">
                                <StatTile
                                    label="Documents"
                                    icon="documents"
                                    iconColor="info"
                                    value={formatNumber(info.CountOfDocuments)}
                                />
                                <StatTile label="Indexes" icon="indexing" value={formatNumber(info.CountOfIndexes)} />
                                <StatTile
                                    label="Attachments"
                                    icon="attachment"
                                    value={formatNumber(info.CountOfAttachments)}
                                />
                                <StatTile
                                    label="Revisions"
                                    icon="revisions"
                                    value={formatNumber(info.CountOfRevisionDocuments)}
                                />
                                <StatTile
                                    label="Conflicts"
                                    icon="conflicts"
                                    value={formatNumber(info.CountOfDocumentsConflicts)}
                                />
                                <StatTile
                                    label="Tombstones"
                                    icon="zombie"
                                    value={formatNumber(info.CountOfTombstones)}
                                />
                                <StatTile
                                    label="Counters"
                                    icon="new-counter"
                                    value={formatNumber(info.CountOfCounterEntries)}
                                />
                                <StatTile
                                    label="Time series segments"
                                    icon="timeseries-settings"
                                    value={formatNumber(info.CountOfTimeSeriesSegments)}
                                />
                                <StatTile
                                    label="Size on disk"
                                    icon="storage"
                                    iconColor="warning"
                                    value={info.SizeOnDisk?.HumaneSize ?? "-"}
                                />
                                <StatTile
                                    label="Temp buffers"
                                    icon="storage"
                                    value={info.TempBuffersSizeOnDisk?.HumaneSize ?? "-"}
                                />
                                <StatTile label="Database ID" icon="hash" value={info.DatabaseId} />
                                <StatTile
                                    label="Last indexing time"
                                    icon="clock"
                                    value={
                                        info.LastIndexingTime
                                            ? genUtils.formatUtcDateAsLocal(info.LastIndexingTime)
                                            : "Never"
                                    }
                                />
                                <StatTile
                                    label="Architecture"
                                    icon="processor"
                                    value={info.Is64Bit ? "64-bit" : "32-bit"}
                                />
                                <StatTile
                                    label="Change vector"
                                    icon="vector"
                                    value={info.DatabaseChangeVector || "-"}
                                />
                            </div>

                            {info.StaleIndexes?.length > 0 && (
                                <div>
                                    <div className="small-label mb-1">Stale indexes ({info.StaleIndexes.length})</div>
                                    <div className="hstack gap-1 flex-wrap">
                                        {info.StaleIndexes.map((name) => (
                                            <Badge key={name} bg="warning">
                                                {name}
                                            </Badge>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>
    );
});
