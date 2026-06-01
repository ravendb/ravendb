import React, { useMemo } from "react";
import { RichAlert } from "components/common/RichAlert";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import Table from "react-bootstrap/Table";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import { ThemeColor } from "components/models/common";
import useBoolean from "components/hooks/useBoolean";
import NodeTagPill from "./NodeTagPill";
import { SortableHeader, useSortableData } from "./sortableTable";

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

const analyzeErrorSortAccessors: Record<string, (error: FlatAnalyzeError) => number | string> = {
    node: (error) => error.nodeTag,
    component: (error) => error.component,
    severity: (error) => severityRank(error.severity),
};

// Surfaces the per-node analyzer failures (node.AnalyzeErrors): components whose package entries
// could not be parsed. A heads-up that the analysis is partial, so missing data is not mistaken
// for an absence of problems.
export default function AnalysisErrors({ summary }: AnalysisErrorsProps) {
    const errors = useMemo(() => flattenAnalyzeErrors(summary), [summary]);
    const { value: detailsVisible, toggle: toggleDetails } = useBoolean(false);
    const { sorted, sortKey, sortDirection, requestSort } = useSortableData(
        errors,
        analyzeErrorSortAccessors,
        "severity"
    );
    const sortProps = { sortKey, sortDirection, onSort: requestSort };

    if (errors.length === 0) {
        return null;
    }

    const variant = errors.some((e) => e.severity === "Error") ? "danger" : "warning";

    return (
        <RichAlert variant={variant} title="The analyzer could not process part of the package">
            <div className="hstack gap-2 mb-1">
                <span>
                    {errors.length} {errors.length === 1 ? "component" : "components"} failed to analyze - some results
                    may be incomplete.
                </span>
                <Button variant="link" size="sm" className="p-0" onClick={toggleDetails}>
                    {detailsVisible ? "Hide" : "Show"} details
                </Button>
            </div>
            <Collapse in={detailsVisible}>
                <div>
                    <Table responsive className="m-0 mt-2 align-middle">
                        <thead>
                            <tr>
                                <SortableHeader label="Node" columnKey="node" {...sortProps} />
                                <SortableHeader label="Component" columnKey="component" {...sortProps} />
                                <SortableHeader label="Severity" columnKey="severity" {...sortProps} />
                                <th>Message</th>
                            </tr>
                        </thead>
                        <tbody>
                            {sorted.map((error) => (
                                <AnalyzeErrorRow key={error.key} error={error} />
                            ))}
                        </tbody>
                    </Table>
                </div>
            </Collapse>
        </RichAlert>
    );
}

function AnalyzeErrorRow({ error }: { error: FlatAnalyzeError }) {
    const { value: exceptionVisible, toggle: toggleException } = useBoolean(false);
    const meta = severityMeta(error.severity);

    return (
        <>
            <tr>
                <td>
                    <NodeTagPill tag={error.nodeTag} />
                </td>
                <td>{error.component}</td>
                <td className={`text-${meta.color}`}>
                    <Icon icon={meta.icon} margin="m-0" /> {error.severity}
                </td>
                <td>
                    {error.message}
                    {error.exception && (
                        <Button variant="link" size="sm" className="p-0 ms-2" onClick={toggleException}>
                            {exceptionVisible ? "Hide" : "Show"} exception
                        </Button>
                    )}
                </td>
            </tr>
            {error.exception && (
                <tr>
                    <td colSpan={4} className="p-0 border-0">
                        <Collapse in={exceptionVisible}>
                            <div>
                                <pre className="debug-package-exception-details mb-0">{error.exception}</pre>
                            </div>
                        </Collapse>
                    </td>
                </tr>
            )}
        </>
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
