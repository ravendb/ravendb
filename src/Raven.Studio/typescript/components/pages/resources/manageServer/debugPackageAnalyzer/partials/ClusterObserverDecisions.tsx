import React, { useMemo, useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";
import Form from "react-bootstrap/Form";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { StatePill } from "components/common/StatePill";
import NodeTagPill from "./NodeTagPill";
import Select, { SelectOption } from "components/common/select/Select";
import genUtils from "common/generalUtils";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type ClusterObserverDecisionsDto = Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions;

interface NodeObserverResult {
    nodeTag: string;
    status: "success" | "failure";
    decisions?: ClusterObserverDecisionsDto;
}

interface ClusterObserverDecisionsProps {
    summary: DebugPackageAnalysisSummary;
}

const maxEntriesShown = 500;

// The cluster observer runs on the leader, so its decision log is captured per node but only meaningful on
// whichever node held leadership. Fetch every node's entry, default to the one with the most decisions (the
// leader), and let the user switch. Mirrors the live Cluster Observer Log view (Date / Database / Message).
export default function ClusterObserverDecisions({ summary }: ClusterObserverDecisionsProps) {
    const { manageServerService } = useServices();
    const packageId = summary.PackageId;
    const nodeTags = useMemo(() => Object.keys(summary.SummaryPerNode ?? {}).sort(), [summary]);

    const observer = useAsync(async () => {
        const settled = await Promise.allSettled(
            nodeTags.map((tag) => manageServerService.getDebugPackageClusterObserverDecisions(packageId, tag))
        );
        return nodeTags.map((tag, index): NodeObserverResult => {
            const outcome = settled[index];
            if (outcome.status === "fulfilled" && outcome.value) {
                return { nodeTag: tag, status: "success", decisions: outcome.value };
            }
            return { nodeTag: tag, status: "failure" };
        });
    }, [packageId, nodeTags]);

    const results = observer.result ?? [];
    const hasAnyDecisions = results.some((result) => result.decisions);

    return (
        <div className="cluster-observer-decisions">
            <h3 className="mb-3">Cluster Observer Decisions</h3>
            {observer.loading ? (
                <Card>
                    <Card.Body>
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> Loading cluster observer decisions...
                        </div>
                    </Card.Body>
                </Card>
            ) : !hasAnyDecisions ? (
                <Card>
                    <Card.Body>
                        <EmptySet compact>No cluster observer decisions in the package</EmptySet>
                    </Card.Body>
                </Card>
            ) : (
                <ObserverBody results={results} />
            )}
        </div>
    );
}

function ObserverBody({ results }: { results: NodeObserverResult[] }) {
    // fulfilled nodes, ranked so the node that actually captured decisions (the leader) is the default
    const ranked = useMemo(
        () =>
            results
                .filter((result) => result.decisions)
                .sort((a, b) => (b.decisions.ObserverLog?.length ?? 0) - (a.decisions.ObserverLog?.length ?? 0)),
        [results]
    );

    const [selectedNode, setSelectedNode] = useState<string>(ranked[0]?.nodeTag ?? null);
    const [filter, setFilter] = useState<string>("");

    const selected = ranked.find((result) => result.nodeTag === selectedNode) ?? ranked[0];
    const decisions = selected?.decisions;
    const log = decisions?.ObserverLog ?? [];

    const filtered = useMemo(() => {
        const needle = filter.trim().toLowerCase();
        const base = needle
            ? log.filter(
                  (entry) =>
                      (entry.Message ?? "").toLowerCase().includes(needle) ||
                      (entry.Database ?? "").toLowerCase().includes(needle)
              )
            : log;
        return [...base].reverse(); // newest first, matching the live view
    }, [log, filter]);

    const shown = filtered.slice(0, maxEntriesShown);

    const nodeOptions: SelectOption<string>[] = ranked.map((result) => ({
        value: result.nodeTag,
        label: `Node ${result.nodeTag} (${(result.decisions.ObserverLog?.length ?? 0).toLocaleString()})`,
    }));

    return (
        <div className="vstack gap-3">
            <Card>
                <Card.Body className="hstack gap-4 flex-wrap align-items-center">
                    <InfoItem
                        label="Leader"
                        value={decisions?.LeaderNode ? <NodeTagPill tag={decisions.LeaderNode} /> : "n/a"}
                    />
                    <InfoItem label="Term" value={decisions?.Term?.toLocaleString() ?? "n/a"} />
                    <InfoItem label="Iteration" value={decisions?.Iteration?.toLocaleString() ?? "n/a"} />
                    <InfoItem
                        label="Observer"
                        value={
                            decisions?.Suspended ? (
                                <StatePill bg="warning">Suspended</StatePill>
                            ) : (
                                <StatePill bg="success">Running</StatePill>
                            )
                        }
                    />
                </Card.Body>
            </Card>

            <Card>
                <Card.Body className="vstack gap-3">
                    <div className="hstack gap-3 align-items-center flex-wrap">
                        <h4 className="m-0">Decisions log</h4>
                        {nodeOptions.length > 1 && (
                            <div className="node-select">
                                <Select
                                    options={nodeOptions}
                                    value={nodeOptions.find((o) => o.value === selected?.nodeTag)}
                                    onChange={(option) => option && setSelectedNode(option.value)}
                                    isSearchable={false}
                                    isRoundedPill
                                />
                            </div>
                        )}
                        <Form.Control
                            type="text"
                            size="sm"
                            style={{ maxWidth: "280px" }}
                            placeholder="Filter by message or database"
                            value={filter}
                            onChange={(e) => setFilter(e.target.value)}
                        />
                        <span className="small-label ms-auto">
                            {filtered.length.toLocaleString()} {filter ? "matching" : "entries"}
                            {filtered.length > maxEntriesShown ? ` (showing first ${maxEntriesShown})` : ""}
                        </span>
                    </div>

                    {shown.length === 0 ? (
                        <EmptySet compact>
                            {filter ? "No decisions match the filter" : "No observer decisions captured for this node"}
                        </EmptySet>
                    ) : (
                        <div style={{ maxHeight: "480px", overflow: "auto" }}>
                            <Table responsive className="m-0 align-middle">
                                <thead>
                                    <tr>
                                        <th>Date</th>
                                        <th>Database</th>
                                        <th>Message</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {shown.map((entry, index) => (
                                        <tr key={index}>
                                            <td className="text-nowrap">
                                                {entry.Date ? genUtils.formatUtcDateAsLocal(entry.Date) : "-"}
                                            </td>
                                            <td>{entry.Database || "-"}</td>
                                            <td>{entry.Message}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </Table>
                        </div>
                    )}
                </Card.Body>
            </Card>
        </div>
    );
}

function InfoItem({ label, value }: { label: string; value: React.ReactNode }) {
    return (
        <div className="vstack">
            <span className="small-label">{label}</span>
            <span>{value}</span>
        </div>
    );
}
