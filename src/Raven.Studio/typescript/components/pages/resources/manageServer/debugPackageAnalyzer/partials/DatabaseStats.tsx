import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import { StatePill } from "components/common/StatePill";
import Select, { SelectOption } from "components/common/select/Select";
import StatTile from "./StatTile";
import genUtils from "common/generalUtils";
import { formatNumber } from "./analyzerUtils";

type DatabaseStatistics = Raven.Client.Documents.Operations.DatabaseStatistics;

interface DatabaseStatsProps {
    packageId: string;
    database: string;
    nodes: string[];
}

// On-demand per-node database statistics from the analyzer databases/stats endpoint - the full
// DatabaseStatistics (counts, sizes, change vector, stale indexes) beyond the per-node overview.
export default function DatabaseStats({ packageId, database, nodes }: DatabaseStatsProps) {
    const { manageServerService } = useServices();
    const [selectedNode, setSelectedNode] = useState<string>(nodes[0] ?? null);

    const stats = useAsync(async () => {
        if (!selectedNode) {
            return null as DatabaseStatistics | null;
        }
        return manageServerService.getDebugPackageDatabaseStats(packageId, selectedNode, database);
    }, [packageId, selectedNode, database]);

    const nodeOptions: SelectOption<string>[] = nodes.map((tag) => ({ value: tag, label: `Node ${tag}` }));
    const info = stats.result;

    return (
        <div className="database-stats">
            <div className="hstack gap-3 align-items-center mb-3 flex-wrap">
                <h3 className="m-0">Statistics</h3>
                {nodes.length > 1 && (
                    <div className="node-select">
                        <Select
                            options={nodeOptions}
                            value={nodeOptions.find((o) => o.value === selectedNode)}
                            onChange={(option) => option && setSelectedNode(option.value)}
                            isSearchable={false}
                            isRoundedPill
                        />
                    </div>
                )}
            </div>
            <Card>
                <Card.Body className="vstack gap-3">
                    {stats.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> Loading statistics for node {selectedNode}...
                        </div>
                    ) : stats.error ? (
                        <RichAlert variant="danger">
                            Could not load statistics for node {selectedNode}. The package may not contain stats for
                            this database, or the report expired.
                        </RichAlert>
                    ) : !info ? (
                        <EmptySet compact>
                            No statistics for {database} on node {selectedNode}
                        </EmptySet>
                    ) : (
                        <>
                            <div className="overview-stats d-flex gap-2 flex-wrap">
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
                            </div>

                            <div className="d-flex gap-4 flex-wrap">
                                <Detail label="Database ID" value={info.DatabaseId} />
                                <Detail
                                    label="Last indexing time"
                                    value={
                                        info.LastIndexingTime
                                            ? genUtils.formatUtcDateAsLocal(info.LastIndexingTime)
                                            : "Never"
                                    }
                                />
                                <Detail label="Architecture" value={info.Is64Bit ? "64-bit" : "32-bit"} />
                            </div>

                            <Detail label="Change vector" value={info.DatabaseChangeVector || "-"} breakAll />

                            {info.StaleIndexes?.length > 0 && (
                                <div>
                                    <div className="small-label mb-1">Stale indexes ({info.StaleIndexes.length})</div>
                                    <div className="hstack gap-1 flex-wrap">
                                        {info.StaleIndexes.map((name) => (
                                            <StatePill key={name} bg="warning">
                                                {name}
                                            </StatePill>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </>
                    )}
                </Card.Body>
            </Card>
        </div>
    );
}

function Detail({ label, value, breakAll }: { label: string; value: string; breakAll?: boolean }) {
    return (
        <div>
            <div className="small-label mb-1">{label}</div>
            <div className={breakAll ? "text-break" : ""}>{value}</div>
        </div>
    );
}
