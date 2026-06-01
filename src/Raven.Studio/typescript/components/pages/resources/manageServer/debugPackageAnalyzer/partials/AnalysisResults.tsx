import React, { useLayoutEffect, useMemo, useRef, useState } from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { StatePill } from "components/common/StatePill";
import NodeTagPill from "./NodeTagPill";
import { RichAlert } from "components/common/RichAlert";
import { EmptySet } from "components/common/EmptySet";
import Button from "react-bootstrap/Button";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import Select, { SelectOption } from "components/common/select/Select";
import { InputItem } from "components/models/common";
import IconName from "typings/server/icons";
import { ThemeColor } from "components/models/common";
import { FlatIssue, countBy, issueCategories, issueScopes, scopeLabel, severityOrder } from "./analyzerUtils";

type IssueSeverity = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueSeverity;

type GroupBy = "severity" | "category" | "none";

const cardSeverities: IssueSeverity[] = ["Error", "Warning", "Info"];

const groupByOptions: SelectOption<GroupBy>[] = [
    { value: "severity", label: "Severity" },
    { value: "category", label: "Category" },
    { value: "none", label: "None" },
];

interface AnalysisResultsProps {
    issues: FlatIssue[];
}

export default function AnalysisResults({ issues }: AnalysisResultsProps) {
    const [category, setCategory] = useState<string>("all");
    const [scope, setScope] = useState<string>("all");
    const [groupBy, setGroupBy] = useState<GroupBy>("severity");
    const [expandedSeverity, setExpandedSeverity] = useState<IssueSeverity>(null);

    const filtered = useMemo(
        () =>
            issues.filter(
                (issue) =>
                    (category === "all" || issue.category === category) && (scope === "all" || issue.scope === scope)
            ),
        [issues, category, scope]
    );

    const categoryCounts = useMemo(() => countBy(issues, (i) => i.category), [issues]);
    const scopeCounts = useMemo(() => countBy(issues, (i) => i.scope), [issues]);
    const severityCounts = useMemo(() => countBy(filtered, (i) => i.severity), [filtered]);

    const categoryItems: InputItem<string>[] = [
        { label: "All", value: "all", count: issues.length },
        ...issueCategories.map((c) => ({ label: c, value: c, count: categoryCounts[c] ?? 0 })),
    ];

    const scopeItems: InputItem<string>[] = [
        { label: "All", value: "all", count: issues.length },
        ...issueScopes.map((s) => ({ label: scopeLabel(s), value: s, count: scopeCounts[s] ?? 0 })),
    ];

    return (
        <div className="analysis-results">
            <h3 className="mb-3">Analysis Results</h3>

            <div className="d-flex gap-4 flex-wrap align-items-end mb-3">
                <MultiRadioToggle<string>
                    label="Filter by issue category"
                    inputItems={categoryItems}
                    selectedItem={category}
                    setSelectedItem={setCategory}
                />
                <MultiRadioToggle<string>
                    label="Filter by scope"
                    inputItems={scopeItems}
                    selectedItem={scope}
                    setSelectedItem={setScope}
                />
                <div className="group-by-control">
                    <div className="small-label ms-1 mb-1">Group by</div>
                    <Select
                        options={groupByOptions}
                        value={groupByOptions.find((o) => o.value === groupBy)}
                        onChange={(option) => option && setGroupBy(option.value)}
                        isSearchable={false}
                        isRoundedPill
                    />
                </div>
            </div>

            {filtered.length === 0 ? (
                <EmptySet>No analysis results match the selected filters</EmptySet>
            ) : groupBy === "severity" ? (
                <SeverityView
                    issues={filtered}
                    severityCounts={severityCounts}
                    expandedSeverity={expandedSeverity}
                    setExpandedSeverity={setExpandedSeverity}
                />
            ) : groupBy === "category" ? (
                <CategoryView issues={filtered} />
            ) : (
                <IssueList issues={sortBySeverity(filtered)} />
            )}
        </div>
    );
}

interface SeverityViewProps {
    issues: FlatIssue[];
    severityCounts: Record<string, number>;
    expandedSeverity: IssueSeverity;
    setExpandedSeverity: (severity: IssueSeverity) => void;
}

function SeverityView({ issues, severityCounts, expandedSeverity, setExpandedSeverity }: SeverityViewProps) {
    return (
        <div className="vstack gap-3">
            <div className="severity-cards d-flex gap-3 flex-wrap">
                {cardSeverities.map((severity) => (
                    <SeverityCard
                        key={severity}
                        severity={severity}
                        count={severityCounts[severity] ?? 0}
                        active={expandedSeverity === severity}
                        onClick={() => setExpandedSeverity(expandedSeverity === severity ? null : severity)}
                    />
                ))}
            </div>
            {expandedSeverity && <IssueList issues={issues.filter((i) => i.severity === expandedSeverity)} />}
        </div>
    );
}

function CategoryView({ issues }: { issues: FlatIssue[] }) {
    const categories = issueCategories.filter((c) => issues.some((i) => i.category === c));
    const otherCategories = Array.from(new Set(issues.map((i) => i.category))).filter(
        (c) => !issueCategories.includes(c)
    );

    return (
        <div className="vstack gap-4">
            {[...categories, ...otherCategories].map((category) => (
                <div key={category}>
                    <h4 className="mb-2">{category}</h4>
                    <IssueList issues={sortBySeverity(issues.filter((i) => i.category === category))} />
                </div>
            ))}
        </div>
    );
}

function IssueList({ issues }: { issues: FlatIssue[] }) {
    return (
        <div className="issue-list">
            {issues.map((issue) => (
                <IssueRow key={issue.key} issue={issue} />
            ))}
        </div>
    );
}

function IssueRow({ issue }: { issue: FlatIssue }) {
    const meta = severityMeta(issue.severity);

    return (
        <div className="issue-row d-flex gap-3 align-items-start py-2">
            <Icon icon={meta.icon} color={meta.color} className="fs-4 mt-1" margin="m-0" />
            <div className="vstack flex-grow-1">
                <div className="fw-bold">{issue.title}</div>
                <IssueDescription text={issue.description} />
                {issue.recommendedAction && (
                    <div className="mt-1">
                        <strong>Recommended action:</strong> {issue.recommendedAction}
                    </div>
                )}
            </div>
            <ScopeBadges issue={issue} />
        </div>
    );
}

// some descriptions (e.g. cluster topology alerts) embed full stack traces - keep the row compact and let the
// user expand. "Show more" appears only when the text actually overflows the clamped height, not by guessing length.
function IssueDescription({ text }: { text: string }) {
    const ref = useRef<HTMLDivElement>(null);
    const [expanded, setExpanded] = useState(false);
    const [overflowing, setOverflowing] = useState(false);

    useLayoutEffect(() => {
        const el = ref.current;
        if (el && !expanded) {
            setOverflowing(el.scrollHeight > el.clientHeight + 1);
        }
    }, [text, expanded]);

    if (!text) {
        return null;
    }

    return (
        <div>
            <div ref={ref} className={classNames("issue-description text-muted", expanded ? "expanded" : "clamped")}>
                {text}
            </div>
            {(overflowing || expanded) && (
                <Button variant="link" size="sm" className="p-0" onClick={() => setExpanded((prev) => !prev)}>
                    {expanded ? "Show less" : "Show more"}
                </Button>
            )}
        </div>
    );
}

function ScopeBadges({ issue }: { issue: FlatIssue }) {
    if (issue.scope === "cluster-wide") {
        return <StatePill bg="info">Cluster-Wide</StatePill>;
    }

    if (issue.scope === "node") {
        return <NodeTagPill tag={issue.nodeTag} />;
    }

    return (
        <div className="hstack gap-1 align-items-start">
            {issue.nodeTag && <NodeTagPill tag={issue.nodeTag} />}
            <StatePill bg="orchestrator">Database {issue.database}</StatePill>
        </div>
    );
}

interface SeverityCardProps {
    severity: IssueSeverity;
    count: number;
    active: boolean;
    onClick: () => void;
}

function SeverityCard({ severity, count, active, onClick }: SeverityCardProps) {
    const meta = severityMeta(severity);

    return (
        <RichAlert
            variant={meta.variant}
            className={classNames("severity-card", { active })}
            childrenClassName="d-flex align-items-center"
            onClick={onClick}
        >
            <span className="severity-card-count me-2">{count}</span>
            <span>{count === 1 ? meta.label : meta.plural}</span>
            <Icon icon={active ? "chevron-down" : "chevron-right"} margin="m-0" className="ms-auto" />
        </RichAlert>
    );
}

interface SeverityMeta {
    icon: IconName;
    color: ThemeColor;
    variant: "danger" | "warning" | "info" | "secondary";
    label: string;
    plural: string;
}

function severityMeta(severity: IssueSeverity): SeverityMeta {
    switch (severity) {
        case "Error":
            return { icon: "danger", color: "danger", variant: "danger", label: "Error", plural: "Errors" };
        case "Warning":
            return { icon: "warning", color: "warning", variant: "warning", label: "Warning", plural: "Warnings" };
        case "Info":
            return { icon: "info", color: "info", variant: "info", label: "Info", plural: "Info" };
        default:
            return { icon: "info", color: "muted", variant: "secondary", label: "Other", plural: "Other" };
    }
}

function sortBySeverity(issues: FlatIssue[]): FlatIssue[] {
    return [...issues].sort((a, b) => severityOrder.indexOf(a.severity) - severityOrder.indexOf(b.severity));
}
