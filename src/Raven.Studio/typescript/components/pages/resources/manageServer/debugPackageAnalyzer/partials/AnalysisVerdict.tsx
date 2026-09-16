import React from "react";
import { FlatIssue, summarizeIssues } from "./analyzerUtils";
import Badge from "react-bootstrap/Badge";
import "./AnalysisVerdict.scss";

type IssueSeverity = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueSeverity;
type VerdictVariant = "danger" | "warning" | "info" | "success";

const SEP = " · ";

interface VerdictMeta {
    bg: VerdictVariant;
    label: string;
}

function verdictMeta(worst: IssueSeverity): VerdictMeta {
    switch (worst) {
        case "Error":
            return { bg: "danger", label: "Needs attention" };
        case "Warning":
            return { bg: "warning", label: "Review recommended" };
        case "Info":
            return { bg: "info", label: "Informational" };
        default:
            return { bg: "success", label: "Healthy" };
    }
}

interface AnalysisVerdictProps {
    issues: FlatIssue[];
    onViewIssues?: () => void;
}

export default function AnalysisVerdict({ issues }: AnalysisVerdictProps) {
    const { total, counts, worst } = summarizeIssues(issues);
    const meta = verdictMeta(worst);
    const healthy = total === 0;

    const segments = [
        { variant: "error", count: counts.Error },
        { variant: "warning", count: counts.Warning },
        { variant: "info", count: counts.Info },
    ].filter((segment) => segment.count > 0);

    const countParts = [
        counts.Error > 0 ? `${counts.Error} ${counts.Error === 1 ? "error" : "errors"}` : null,
        counts.Warning > 0 ? `${counts.Warning} ${counts.Warning === 1 ? "warning" : "warnings"}` : null,
        counts.Info > 0 ? `${counts.Info} info` : null,
    ].filter((part): part is string => part !== null);

    const findings = `${total} ${total === 1 ? "finding" : "findings"}`;
    const caption = healthy ? "No issues found in this package" : `${countParts.join(SEP)}`;
    const meterLabel = healthy ? caption : `${findings}: ${countParts.join(", ")}`;

    return (
        <div className="analysis-verdict">
            <Badge bg={meta.bg} pill>
                {meta.label}
            </Badge>
            <div className="small-label">{caption}</div>
            <div className="analysis-verdict-body">
                <div className="severity-meter" role="img" aria-label={meterLabel}>
                    {healthy ? (
                        <span
                            className="severity-meter-segment severity-meter-segment--success"
                            style={{ width: "100%" }}
                            aria-hidden="true"
                        />
                    ) : (
                        segments.map((segment) => (
                            <span
                                key={segment.variant}
                                className={`severity-meter-segment severity-meter-segment--${segment.variant}`}
                                style={{ width: `${(segment.count / total) * 100}%` }}
                                aria-hidden="true"
                            />
                        ))
                    )}
                </div>
            </div>
        </div>
    );
}
