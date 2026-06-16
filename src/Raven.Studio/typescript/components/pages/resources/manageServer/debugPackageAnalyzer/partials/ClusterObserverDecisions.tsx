import { useMemo, useState } from "react";
import Spinner from "react-bootstrap/Spinner";
import Form from "react-bootstrap/Form";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import NodeTagPill from "./NodeTagPill";
import StatTile from "./StatTile";
import Select, { SelectOption } from "components/common/select/Select";
import genUtils from "common/generalUtils";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type ClusterObserverDecisionsDto = Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions;
type ClusterObserverLogEntry = Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry;

interface NodeObserverResult {
    nodeTag: string;
    status: "success" | "failure";
    decisions?: ClusterObserverDecisionsDto;
}

const maxObserverEntriesShown = 2_000;

interface ClusterObserverDecisionsProps {
    summary: DebugPackageAnalysisSummary;
}

interface ObserverBodyProps {
    results: NodeObserverResult[];
    width: number;
}

function ObserverBody({ results, width }: ObserverBodyProps) {
    // fulfilled nodes, ranked so the node that actually captured decisions (the leader) is the default
    const ranked = useMemo(
        () =>
            results
                .filter(
                    (result): result is NodeObserverResult & { decisions: ClusterObserverDecisionsDto } =>
                        !!result.decisions
                )
                .sort((a, b) => (b.decisions.ObserverLog?.length ?? 0) - (a.decisions.ObserverLog?.length ?? 0)),
        [results]
    );

    const [selectedNode, setSelectedNode] = useState<string>(ranked[0]?.nodeTag ?? null);
    const [filter, setFilter] = useState<string>("");

    const selected = ranked.find((result) => result.nodeTag === selectedNode);
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
        // newest first; slice before reversing so we only copy at most maxObserverEntriesShown entries
        return base.length > maxObserverEntriesShown
            ? base.slice(-maxObserverEntriesShown).reverse()
            : [...base].reverse();
    }, [log, filter]);

    const totalEntries = log.length;

    const nodeOptions: SelectOption<string>[] = ranked.map((result) => ({
        value: result.nodeTag,
        label: `Node ${result.nodeTag} (${(result.decisions.ObserverLog?.length ?? 0).toLocaleString()})`,
    }));

    const { logColumns } = useLogColumns(width);

    const table = useReactTable({
        data: filtered,
        columns: logColumns,
        enableSorting: filtered.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: filtered.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (_row, index) => String(index),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(filtered.length, 500);

    return (
        <div className="vstack gap-3">
            <div className="hstack gap-3 flex-wrap">
                <StatTile
                    className="flex-fill"
                    label="Leader node"
                    icon="node-leader"
                    value={decisions?.LeaderNode ? <NodeTagPill tag={decisions.LeaderNode} /> : "n/a"}
                />
                <StatTile
                    className="flex-fill"
                    label="Term"
                    icon="cluster"
                    value={decisions?.Term?.toLocaleString() ?? "n/a"}
                />
                <StatTile
                    className="flex-fill"
                    label="Iteration"
                    icon="refresh"
                    value={decisions?.Iteration?.toLocaleString() ?? "n/a"}
                />
                <StatTile
                    className="flex-fill"
                    label="Observer"
                    icon="play"
                    iconColor={decisions?.Suspended ? "warning" : "success"}
                    value={decisions?.Suspended ? "Suspended" : "Running"}
                    valueColor={decisions?.Suspended ? "warning" : "success"}
                />
            </div>

            <div className="vstack gap-2">
                <h4 className="m-0">Decisions log</h4>
                <div className="hstack gap-2 align-items-end flex-wrap">
                    {nodeOptions.length > 1 && (
                        <div>
                            <div className="small-label ms-1 mb-1">Node</div>
                            <div className="node-select">
                                <Select
                                    options={nodeOptions}
                                    value={nodeOptions.find((o) => o.value === selected?.nodeTag)}
                                    onChange={(option) => option && setSelectedNode(option.value)}
                                    isSearchable={false}
                                    isRoundedPill
                                />
                            </div>
                        </div>
                    )}
                    <div>
                        <div className="small-label mb-1">Filter by message</div>
                        <Form.Control
                            type="text"
                            size="sm"
                            style={{ maxWidth: "400px" }}
                            placeholder="Filter by message or database"
                            value={filter}
                            onChange={(e) => setFilter(e.target.value)}
                        />
                    </div>
                    <span className="small-label ms-auto">
                        {filtered.length.toLocaleString()} {filter ? "matching" : "entries"}
                        {!filter && totalEntries > maxObserverEntriesShown && (
                            <>
                                {" "}
                                (showing latest {maxObserverEntriesShown.toLocaleString()} of{" "}
                                {totalEntries.toLocaleString()})
                            </>
                        )}
                    </span>
                </div>

                {filtered.length === 0 ? (
                    <EmptySet compact className="justify-content-center">
                        {filter ? "No decisions match the filter" : "No observer decisions captured for this node"}
                    </EmptySet>
                ) : (
                    <VirtualTable table={table} heightInPx={heightInPx} />
                )}
            </div>
        </div>
    );
}

// The exported component passes width down so ObserverBody can size its table columns proportionally.
// ClusterObserverDecisions itself doesn't render a table directly, so SizeGetter wraps ObserverBody.
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
        <div className="panel-bg-1 rounded cluster-observer-decisions">
            <div className="p-4">
                <h3 className="mb-3">Cluster Observer Decisions</h3>
                {observer.loading ? (
                    <div className="hstack gap-2 justify-content-center text-muted py-3">
                        <Spinner size="sm" /> Loading cluster observer decisions...
                    </div>
                ) : !hasAnyDecisions ? (
                    <EmptySet compact className="justify-content-center">
                        No cluster observer decisions in the package
                    </EmptySet>
                ) : (
                    <SizeGetter render={({ width }) => <ObserverBody results={results} width={width} />} />
                )}
            </div>
        </div>
    );
}

function useLogColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const logColumns: ColumnDef<ClusterObserverLogEntry>[] = useMemo(
        () => [
            {
                header: "Date",
                accessorKey: "Date",
                cell: ({ getValue }) => {
                    const v = getValue<string>();
                    return v ? genUtils.formatUtcDateAsLocal(v) : "-";
                },
                size: getSize(20),
            },
            {
                header: "Database",
                accessorKey: "Database",
                cell: ({ getValue }) => {
                    const v = getValue<string>();
                    return <span title={v || undefined}>{v || "-"}</span>;
                },
                size: getSize(20),
            },
            {
                header: "Message",
                accessorKey: "Message",
                cell: ({ getValue }) => {
                    const v = getValue<string>();
                    return <span title={v}>{v}</span>;
                },
                size: getSize(60),
            },
        ],
        [getSize]
    );

    return { logColumns };
}
