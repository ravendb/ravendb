import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import { StatePill } from "components/common/StatePill";
import Select, { SelectOption } from "components/common/select/Select";

type IndexStats = Raven.Client.Documents.Indexes.IndexStats;
type IndexState = Raven.Client.Documents.Indexes.IndexState;

interface DatabaseIndexStatsProps {
    packageId: string;
    database: string;
    nodes: string[];
}

// On-demand per-node index stats for the selected database, from the analyzer
// databases/indexes/stats endpoint (the summary only has aggregate indexing speed).
export default function DatabaseIndexStats({ packageId, database, nodes }: DatabaseIndexStatsProps) {
    const { manageServerService } = useServices();
    const [selectedNode, setSelectedNode] = useState<string>(nodes[0] ?? null);

    const stats = useAsync(async () => {
        if (!selectedNode) {
            return [] as IndexStats[];
        }
        return manageServerService.getDebugPackageDatabaseIndexStats(packageId, selectedNode, database);
    }, [packageId, selectedNode, database]);

    const nodeOptions: SelectOption<string>[] = nodes.map((tag) => ({ value: tag, label: `Node ${tag}` }));
    const indexes = stats.result ?? [];

    return (
        <div className="database-index-stats">
            <div className="hstack gap-3 align-items-center mb-3 flex-wrap">
                <h3 className="m-0">Indexes</h3>
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
                <Card.Body>
                    {stats.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> Loading indexes for node {selectedNode}...
                        </div>
                    ) : stats.error ? (
                        <RichAlert variant="danger">
                            Could not load index stats for node {selectedNode}. The package may not contain index data
                            for this database, or the report expired.
                        </RichAlert>
                    ) : indexes.length === 0 ? (
                        <EmptySet compact>
                            No indexes for {database} on node {selectedNode}
                        </EmptySet>
                    ) : (
                        <Table responsive className="m-0 align-middle">
                            <thead>
                                <tr>
                                    <th>Index</th>
                                    <th>State</th>
                                    <th>Priority</th>
                                    <th>Type</th>
                                    <th>Entries</th>
                                    <th>Errors</th>
                                    <th>Stale</th>
                                    <th>Lock mode</th>
                                </tr>
                            </thead>
                            <tbody>
                                {indexes.map((index) => (
                                    <tr key={index.Name}>
                                        <td className="fw-bold">
                                            <div className="text-truncate index-name" title={index.Name}>
                                                {index.Name}
                                            </div>
                                        </td>
                                        <td>{statePill(index.State)}</td>
                                        <td>{index.Priority}</td>
                                        <td>{index.Type}</td>
                                        <td>{formatCount(index.EntriesCount)}</td>
                                        <td className={index.ErrorsCount > 0 ? "text-danger" : ""}>
                                            {formatCount(index.ErrorsCount)}
                                        </td>
                                        <td>
                                            {index.IsStale ? <StatePill bg="warning">Stale</StatePill> : "Up to date"}
                                        </td>
                                        <td>{index.LockMode}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </Table>
                    )}
                </Card.Body>
            </Card>
        </div>
    );
}

function formatCount(value: number): string {
    return value == null ? "-" : value.toLocaleString();
}

function statePill(state: IndexState) {
    switch (state) {
        case "Error":
            return <StatePill bg="danger">Error</StatePill>;
        case "Disabled":
            return <StatePill bg="warning">Disabled</StatePill>;
        case "Idle":
            return <StatePill bg="secondary">Idle</StatePill>;
        default:
            return <StatePill bg="success">Normal</StatePill>;
    }
}
