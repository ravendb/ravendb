import React, { useMemo, useRef } from "react";
import { RichAlert } from "components/common/RichAlert";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import { ThemeColor } from "components/models/common";
import useBoolean from "components/hooks/useBoolean";
import NodeTagPill from "./NodeTagPill";
import DebugPackageDetailsSheet from "./DebugPackageDetailsSheet";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useResizeObserver } from "hooks/useResizeObserver";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { ColumnDef, getCoreRowModel, useReactTable } from "@tanstack/react-table";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type AnalyzeErrorSeverity =
    Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors.AnalyzeErrorSeverity;

interface FlatAnalyzeError {
    key: string;
    nodeTag: string;
    component: string;
    message: string;
    exception: string;
    severity: AnalyzeErrorSeverity;
}

interface AnalysisErrorsProps {
    summary: DebugPackageAnalysisSummary;
}

function severityRank(severity: AnalyzeErrorSeverity): number {
    switch (severity) {
        case "Error":
            return 2;
        case "Warning":
            return 1;
        default:
            return 0;
    }
}

// Surfaces the per-node analyzer failures (node.AnalyzeErrors): components whose package entries
// could not be parsed. A heads-up that the analysis is partial, so missing data is not mistaken
// for an absence of problems.
export default function AnalysisErrors({ summary }: AnalysisErrorsProps) {
    const errors = useMemo(
        () => flattenAnalyzeErrors(summary).sort((a, b) => severityRank(b.severity) - severityRank(a.severity)),
        [summary]
    );
    const { value: detailsVisible, toggle: toggleDetails } = useBoolean(false);
    const { open } = useViewSheet();

    if (errors.length === 0) {
        return null;
    }

    const variant = errors.some((e) => e.severity === "Error") ? "danger" : "warning";

    const showException = (error: FlatAnalyzeError) => {
        const meta = severityMeta(error.severity);
        open({
            component: (
                <DebugPackageDetailsSheet
                    title={`${error.component} on Node ${error.nodeTag}`}
                    content={error.exception}
                    icon={meta.icon}
                    iconColor={meta.color}
                />
            ),
        });
    };

    return (
        <RichAlert variant={variant} title="The analyzer could not process part of the package">
            <div className="hstack gap-2">
                <span>
                    {errors.length} {errors.length === 1 ? "component" : "components"} failed to analyze - some results
                    may be incomplete.
                </span>
                <Button variant="link" size="sm" className="p-0" onClick={toggleDetails}>
                    {detailsVisible ? "Hide" : "Show"} details
                </Button>
            </div>
            {detailsVisible && (
                <div className="mt-2">
                    <AnalysisErrorsTable errors={errors} onShowException={showException} />
                </div>
            )}
        </RichAlert>
    );
}

function AnalysisErrorsTable({
    errors,
    onShowException,
}: {
    errors: FlatAnalyzeError[];
    onShowException: (error: FlatAnalyzeError) => void;
}) {
    const containerRef = useRef<HTMLDivElement>(null);
    const { width } = useResizeObserver({ ref: containerRef });

    const bodyWidth = virtualTableUtils.getTableBodyWidth(width || 0);
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const columns = useMemo<ColumnDef<FlatAnalyzeError>[]>(
        () => [
            {
                id: "node",
                header: "Node",
                accessorKey: "nodeTag",
                size: getSize(10),
                cell: ({ getValue }) => <NodeTagPill tag={getValue() as string} />,
            },
            {
                id: "component",
                header: "Component",
                accessorKey: "component",
                size: getSize(20),
            },
            {
                id: "severity",
                header: "Severity",
                accessorKey: "severity",
                size: getSize(15),
                cell: ({ row }) => {
                    const meta = severityMeta(row.original.severity);
                    return (
                        <span className={`text-${meta.color}`}>
                            <Icon icon={meta.icon} margin="m-0" /> {row.original.severity}
                        </span>
                    );
                },
            },
            {
                id: "message",
                header: "Message",
                accessorKey: "message",
                size: getSize(55),
                cell: ({ row }) => (
                    <div className="d-flex gap-2 align-items-center w-100">
                        <span className="text-truncate" title={row.original.message}>
                            {row.original.message}
                        </span>
                        {row.original.exception && (
                            <Button
                                variant="link"
                                size="sm"
                                className="p-0 ms-auto flex-shrink-0"
                                onClick={() => onShowException(row.original)}
                            >
                                View exception
                            </Button>
                        )}
                    </div>
                ),
            },
        ],
        [getSize, onShowException]
    );

    const table = useReactTable({
        data: errors,
        columns,
        enableSorting: false,
        enableColumnFilters: false,
        getCoreRowModel: getCoreRowModel(),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(errors.length, 400);

    return (
        <div ref={containerRef} className="w-100">
            {width ? <VirtualTable table={table} heightInPx={heightInPx} /> : null}
        </div>
    );
}

function severityMeta(severity: AnalyzeErrorSeverity): { icon: IconName; color: ThemeColor } {
    switch (severity) {
        case "Error":
            return { icon: "danger", color: "danger" };
        case "Warning":
            return { icon: "warning", color: "warning" };
        default:
            return { icon: "info", color: "info" };
    }
}

export function hasAnalyzeErrors(summary: DebugPackageAnalysisSummary): boolean {
    return Object.values(summary.SummaryPerNode ?? {}).some((node) => (node.AnalyzeErrors?.Errors ?? []).length > 0);
}

function flattenAnalyzeErrors(summary: DebugPackageAnalysisSummary): FlatAnalyzeError[] {
    const result: FlatAnalyzeError[] = [];
    let counter = 0;

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        (node.AnalyzeErrors?.Errors ?? []).forEach((error) => {
            result.push({
                key: `${nodeTag}-${counter++}`,
                nodeTag,
                component: error.ComponentName,
                message: error.ErrorMessage,
                exception: error.Exception,
                severity: error.Severity,
            });
        });
    });

    return result;
}
