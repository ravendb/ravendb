import React, { useLayoutEffect, useMemo, useRef, useState } from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import Badge from "react-bootstrap/Badge";
import NodeTagPill from "./NodeTagPill";
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
    const [expandedSeverity, setExpandedSeverity] = useState<IssueSeverity | null>(null);

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
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack">
                    <h3 className="mb-3">Analysis Results</h3>

                    <div className="d-flex gap-2 flex-wrap align-items-end mb-3">
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
            </div>
        </div>
    );
}

interface SeverityViewProps {
    issues: FlatIssue[];
    severityCounts: Record<string, number>;
    expandedSeverity: IssueSeverity | null;
    setExpandedSeverity: (severity: IssueSeverity | null) => void;
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
            {expandedSeverity && <GroupedIssueList issues={issues.filter((i) => i.severity === expandedSeverity)} />}
        </div>
    );
}

function CategoryView({ issues }: { issues: FlatIssue[] }) {
    const [expandedCategory, setExpandedCategory] = useState<string | null>(null);

    const categories = issueCategories.filter((c) => issues.some((i) => i.category === c));
    const otherCategories = Array.from(new Set(issues.map((i) => i.category))).filter(
        (c) => !issueCategories.includes(c)
    );
    const allCategories = [...categories, ...otherCategories];

    return (
        <div className="vstack gap-3">
            <div className="severity-cards d-flex gap-3 flex-wrap">
                {allCategories.map((category) => {
                    const categoryIssues = issues.filter((i) => i.category === category);
                    return (
                        <CategoryCard
                            key={category}
                            category={category}
                            count={categoryIssues.length}
                            severity={highestSeverity(categoryIssues)}
                            active={expandedCategory === category}
                            onClick={() => setExpandedCategory(expandedCategory === category ? null : category)}
                        />
                    );
                })}
            </div>
            {expandedCategory && (
                <IssueList issues={sortBySeverity(issues.filter((i) => i.category === expandedCategory))} />
            )}
        </div>
    );
}

interface CategoryCardProps {
    category: string;
    count: number;
    severity: IssueSeverity;
    active: boolean;
    onClick: () => void;
}

function CategoryCard({ category, count, severity, active, onClick }: CategoryCardProps) {
    const meta = severityMeta(severity);
    return <SeverityCardBase meta={meta} count={count} label={category} active={active} onClick={onClick} />;
}

function highestSeverity(issues: FlatIssue[]): IssueSeverity {
    return issues.reduce<IssueSeverity>(
        (worst, issue) =>
            severityOrder.indexOf(issue.severity) < severityOrder.indexOf(worst) ? issue.severity : worst,
        "None"
    );
}

function GroupedIssueList({ issues }: { issues: FlatIssue[] }) {
    const known = issueCategories.filter((c) => issues.some((i) => i.category === c));
    const other = Array.from(new Set(issues.map((i) => i.category))).filter((c) => !issueCategories.includes(c));
    const allCategories = [...known, ...other];

    if (allCategories.length <= 1) {
        return <IssueList issues={issues} />;
    }

    return (
        <div className="vstack gap-2">
            {allCategories.map((cat) => (
                <div key={cat}>
                    <div className="issue-group-header">{cat}</div>
                    <IssueList issues={issues.filter((i) => i.category === cat)} />
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
        <div className="issue-row d-flex gap-3 align-items-start">
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
        return (
            <Badge bg="light">
                <Icon icon="cluster" /> Cluster-Wide
            </Badge>
        );
    }

    const nodePills = issue.nodeTags.map((tag) => <NodeTagPill key={tag} tag={tag} />);

    if (issue.scope === "node") {
        return <div className="hstack gap-1 align-items-center flex-wrap justify-content-end">{nodePills}</div>;
    }

    return (
        <div className="hstack gap-1 align-items-center flex-wrap justify-content-end">
            {nodePills}
            <Badge bg="secondary">
                <Icon icon="database" /> {issue.database}
            </Badge>
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
        <SeverityCardBase
            meta={meta}
            count={count}
            label={count === 1 ? meta.label : meta.plural}
            active={active}
            onClick={onClick}
        />
    );
}

interface SeverityCardBaseProps {
    meta: SeverityMeta;
    count: number;
    label: string;
    active: boolean;
    onClick: () => void;
}

function SeverityCardBase({ meta, count, label, active, onClick }: SeverityCardBaseProps) {
    const isEmpty = count === 0;

    return (
        <button
            type="button"
            className={classNames("severity-card", `severity-card--${meta.variant}`, {
                active,
                "severity-card--empty": isEmpty,
            })}
            onClick={isEmpty ? undefined : onClick}
            disabled={isEmpty}
        >
            <div className="severity-card-content">
                <Icon icon={meta.icon} color={meta.color} margin="m-0" className="flex-shrink-0" />
                <span className="severity-card-text">
                    <strong>{count}</strong> {label}
                </span>
            </div>
            <div className="severity-card-arrow">
                <Icon icon={active ? "chevron-down" : "chevron-right"} margin="m-0" />
            </div>
        </button>
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
            return { icon: "info", color: "info", variant: "info", label: "Info alert", plural: "Info alerts" };
        default:
            return { icon: "info", color: "muted", variant: "secondary", label: "Other", plural: "Other" };
    }
}

function sortBySeverity(issues: FlatIssue[]): FlatIssue[] {
    return [...issues].sort((a, b) => severityOrder.indexOf(a.severity) - severityOrder.indexOf(b.severity));
}
