import React, { useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import Badge from "react-bootstrap/Badge";
import { EmptySet } from "components/common/EmptySet";
import AnalysisResults from "./AnalysisResults";
import AnalysisErrors, { hasAnalyzeErrors } from "./AnalysisErrors";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import ClusterOverview from "./ClusterOverview";
import ClusterObserverDecisions from "./ClusterObserverDecisions";
import ClusterRaftDebug from "./ClusterRaftDebug";
import ResourceUsage from "./ResourceUsage";
import DatabasesOverview from "./DatabasesOverview";
import NodeOverview from "./NodeOverview";
import PerformanceMetrics from "./PerformanceMetrics";
import StoragePerDatabase from "./StoragePerDatabase";
import IndexingPerNode from "./IndexingPerNode";
import OngoingTasks from "./OngoingTasks";
import DatabaseContextView from "./DatabaseContextView";
import { flattenIssues } from "./analyzerUtils";
import Select, { SelectOption } from "components/common/select/Select";
import { components, OptionProps, SingleValueProps } from "react-select";
import { AnalysisSectionsProvider, useAnalysisSections } from "./AnalysisSectionsContext";
import { ExpandAllProvider } from "./ExpandAllContext";
import ExpandAllToggle from "./ExpandAllToggle";
import AnalysisSection from "./AnalysisSection";
import AnalysisNavRail, { ScopeItem } from "./AnalysisNavRail";
import { useScrollSpy } from "hooks/useScrollSpy";
import "./DebugPackageAnalysisView.scss";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type AnalysisContext = "cluster" | "node" | "database";

// A scope selector option that also carries the issue count for that node/database, rendered as a
// badge inside the select control next to the selected name.
type ScopeSelectOption = SelectOption<string> & { count?: number; countTitle?: string };

function ScopeIssueSingleValue({ children, ...props }: SingleValueProps<ScopeSelectOption>) {
    const { count, countTitle } = props.data;
    return (
        <components.SingleValue {...props}>
            <span className="d-flex align-items-center gap-2">
                <span className="text-truncate" style={{ minWidth: 0 }}>
                    {children}
                </span>
                {count != null && (
                    <Badge bg="secondary" pill className="analysis-scope-count flex-shrink-0" title={countTitle}>
                        {count}
                    </Badge>
                )}
            </span>
        </components.SingleValue>
    );
}

// Renders each dropdown option's name with its own issue count badge, aligned to the right.
function ScopeIssueOption({ children, ...props }: OptionProps<ScopeSelectOption>) {
    const { count } = props.data;
    return (
        <components.Option {...props}>
            <span className="d-flex align-items-center gap-3 w-100">
                <span className="text-truncate" style={{ minWidth: 0 }}>
                    {children}
                </span>
                {count != null && (
                    <Badge bg="secondary" pill className="analysis-scope-count ms-auto flex-shrink-0">
                        {count}
                    </Badge>
                )}
            </span>
        </components.Option>
    );
}

interface DebugPackageAnalysisViewProps {
    summary: DebugPackageAnalysisSummary;
}

export default function DebugPackageAnalysisView({ summary }: DebugPackageAnalysisViewProps) {
    return (
        <AnalysisSectionsProvider>
            <ExpandAllProvider>
                <AnalysisBody summary={summary} />
            </ExpandAllProvider>
        </AnalysisSectionsProvider>
    );
}

function AnalysisBody({ summary }: DebugPackageAnalysisViewProps) {
    const nodeTags = useMemo(() => Object.keys(summary.SummaryPerNode ?? {}).sort(), [summary]);
    const databaseNames = useMemo(() => collectDatabaseNames(summary), [summary]);
    const [context, setContext] = useState<AnalysisContext>("cluster");
    const deferredContext = useDeferredValue(context);
    const [selectedNode, setSelectedNode] = useState<string>(() => defaultNode(summary, nodeTags));
    const [selectedDatabase, setSelectedDatabase] = useState<string>(() => databaseNames[0] ?? null);
    const deferredDatabase = useDeferredValue(selectedDatabase);
    const [selectedDatabaseNode, setSelectedDatabaseNode] = useState<string>(() => defaultNode(summary, nodeTags));
    const databaseNodes = useMemo(() => collectDatabaseNodes(summary, deferredDatabase), [summary, deferredDatabase]);
    const databaseNodeOptions = useMemo(
        () => databaseNodes.map((tag): SelectOption<string> => ({ value: tag, label: `Node ${tag}` })),
        [databaseNodes]
    );
    // the node selector only lists nodes that actually host the selected database, so fall back to one of
    // them whenever the previously selected node isn't among the new database's nodes
    const effectiveDatabaseNode = databaseNodes.includes(selectedDatabaseNode)
        ? selectedDatabaseNode
        : (databaseNodes[0] ?? null);

    const allIssues = useMemo(() => flattenIssues(summary), [summary]);

    const contextIssues = useMemo(() => {
        if (deferredContext === "node") {
            return allIssues.filter((issue) => issue.nodeTags.includes(selectedNode));
        }
        if (deferredContext === "database") {
            return allIssues.filter((issue) => issue.scope === "database" && issue.database === deferredDatabase);
        }
        return allIssues;
    }, [allIssues, deferredContext, selectedNode, deferredDatabase]);

    // Issue counts per node and per database, shown as badges on the scope selectors (the selected
    // value and every dropdown option) rather than the generic scope tabs, where a single count would
    // misrepresent the whole scope.
    const nodeIssueCounts = useMemo(() => {
        const counts: Record<string, number> = {};
        nodeTags.forEach((tag) => {
            counts[tag] = allIssues.filter((i) => i.nodeTags.includes(tag)).length;
        });
        return counts;
    }, [allIssues, nodeTags]);
    const databaseIssueCounts = useMemo(() => {
        const counts: Record<string, number> = {};
        databaseNames.forEach((name) => {
            counts[name] = allIssues.filter((i) => i.scope === "database" && i.database === name).length;
        });
        return counts;
    }, [allIssues, databaseNames]);
    const selectedNodeIssueCount = nodeIssueCounts[selectedNode] ?? 0;
    const selectedDatabaseIssueCount = databaseIssueCounts[selectedDatabase] ?? 0;

    const contextItems = useMemo<ScopeItem<AnalysisContext>[]>(
        () => [
            { label: "Cluster", value: "cluster", icon: "cluster", count: allIssues.length },
            { label: "Node", value: "node", icon: "node" },
            { label: "Database", value: "database", icon: "database" },
        ],
        [allIssues.length]
    );

    const { entries } = useAnalysisSections();
    const contentRef = useRef<HTMLDivElement>(null);
    const sectionIds = useMemo(() => entries.map((e) => e.id), [entries]);
    const activeSectionId = useScrollSpy(sectionIds, { root: findScrollParent(contentRef.current) });

    // reset to the top of the page whenever the scope changes
    useEffect(() => {
        const root = findScrollParent(contentRef.current);
        if (root) {
            root.scrollTo({ top: 0 });
        } else {
            window.scrollTo({ top: 0 });
        }
    }, [context]);

    const handleSelectSection = (id: string) => {
        document.getElementById(id)?.scrollIntoView({ block: "start", behavior: "smooth" });
    };

    const hasScopeControls =
        (deferredContext === "node" && nodeTags.length > 0) ||
        (deferredContext === "database" && (databaseNames.length > 0 || nodeTags.length > 1));

    // The global expand/collapse toggle only has something to act on in cluster scope with more than
    // one node: node scope renders the tables single-node-filtered (nothing expandable) and a
    // single-node cluster has no expandable rows, so the toggle would otherwise sit there inert.
    const showExpandAll = context === "cluster" && nodeTags.length > 1;

    const scopeControls = hasScopeControls ? (
        <>
            {deferredContext === "node" && nodeTags.length > 0 && (
                <div>
                    <div className="small-label">Select node</div>
                    <Select<ScopeSelectOption>
                        options={nodeTags.map(
                            (tag): ScopeSelectOption => ({
                                value: tag,
                                label: `Node ${tag}`,
                                count: nodeIssueCounts[tag],
                            })
                        )}
                        value={{
                            value: selectedNode,
                            label: `Node ${selectedNode}`,
                            count: selectedNodeIssueCount,
                            countTitle: "Issues for the selected node",
                        }}
                        onChange={(option) => option && setSelectedNode(option.value)}
                        isSearchable={false}
                        menuPosition="fixed"
                        components={{ SingleValue: ScopeIssueSingleValue, Option: ScopeIssueOption }}
                    />
                </div>
            )}
            {deferredContext === "database" && databaseNames.length > 0 && (
                <div>
                    <div className="small-label">Select database</div>
                    <Select<ScopeSelectOption>
                        options={databaseNames.map(
                            (name): ScopeSelectOption => ({
                                value: name,
                                label: name,
                                count: databaseIssueCounts[name],
                            })
                        )}
                        value={
                            selectedDatabase
                                ? {
                                      value: selectedDatabase,
                                      label: selectedDatabase,
                                      count: selectedDatabaseIssueCount,
                                      countTitle: "Issues for the selected database",
                                  }
                                : null
                        }
                        onChange={(option) => option && setSelectedDatabase(option.value)}
                        isSearchable
                        isLoading={deferredDatabase !== selectedDatabase}
                        menuPosition="fixed"
                        components={{ SingleValue: ScopeIssueSingleValue, Option: ScopeIssueOption }}
                    />
                </div>
            )}
            {deferredContext === "database" && nodeTags.length > 1 && (
                <PopoverWithHoverWrapper
                    message={databaseNodes.length <= 1 ? "This database has data on one node only" : null}
                    placement="top"
                    overlayProps={{
                        popperConfig: {
                            modifiers: [{ name: "offset", options: { offset: [0, -16] } }],
                        },
                    }}
                    inline={false}
                    targetStyle={{ width: "100%" }}
                >
                    <div>
                        <div className="small-label">Select node</div>
                        <Select<SelectOption<string>>
                            options={databaseNodeOptions}
                            value={
                                effectiveDatabaseNode
                                    ? { value: effectiveDatabaseNode, label: `Node ${effectiveDatabaseNode}` }
                                    : null
                            }
                            onChange={(option) => option && setSelectedDatabaseNode(option.value)}
                            isSearchable={false}
                            isDisabled={databaseNodes.length <= 1}
                            menuPosition="fixed"
                        />
                    </div>
                </PopoverWithHoverWrapper>
            )}
        </>
    ) : null;

    return (
        <div className="debug-package-analysis analysis-layout">
            <AnalysisNavRail<AnalysisContext>
                scopeItems={contextItems}
                selectedScope={context}
                onSelectScope={setContext}
                globalControls={showExpandAll ? <ExpandAllToggle /> : null}
                scopeControls={scopeControls}
                sections={entries}
                activeSectionId={activeSectionId}
                onSelectSection={handleSelectSection}
            />

            <div className="analysis-content vstack" ref={contentRef}>
                {hasAnalyzeErrors(summary) && (
                    <AnalysisSection id="analysis-errors" label="Analysis Errors" className="mb-3">
                        <AnalysisErrors summary={summary} />
                    </AnalysisSection>
                )}

                <AnalysisSection id="debug-package-analysis-results" label="Analysis Results" className="mb-3">
                    <AnalysisResults issues={contextIssues} />
                </AnalysisSection>

                {deferredContext === "cluster" && (
                    <>
                        <AnalysisSection id="cluster-overview" label="Cluster Overview" className="mb-3">
                            <ClusterOverview summary={summary} />
                        </AnalysisSection>
                        <AnalysisSection id="resource-usage" label="Resource Usage" className="mb-3">
                            <ResourceUsage summary={summary} />
                        </AnalysisSection>
                        <AnalysisSection id="databases-overview" label="Databases Overview" className="mb-3">
                            <DatabasesOverview summary={summary} />
                        </AnalysisSection>
                        <AnalysisSection id="storage-per-database" label="Storage per Database" className="mb-3">
                            <StoragePerDatabase summary={summary} />
                        </AnalysisSection>
                        <AnalysisSection id="indexing-per-node" label="Indexing per Node" className="mb-3">
                            <IndexingPerNode summary={summary} />
                        </AnalysisSection>
                        <AnalysisSection id="ongoing-tasks" label="Ongoing Tasks" className="mb-3">
                            <OngoingTasks summary={summary} />
                        </AnalysisSection>
                        <AnalysisSection id="raft-debug" label="Cluster Debug" className="mb-3">
                            <ClusterRaftDebug summary={summary} />
                        </AnalysisSection>
                        <AnalysisSection id="observer-decisions" label="Cluster Observer Decisions" className="mb-3">
                            <ClusterObserverDecisions summary={summary} />
                        </AnalysisSection>
                    </>
                )}

                {deferredContext === "node" && selectedNode && (
                    <>
                        <AnalysisSection id="node-overview" label="Node Overview" className="mb-3">
                            <NodeOverview summary={summary} nodeTag={selectedNode} />
                        </AnalysisSection>
                        <AnalysisSection id="performance-metrics" label="Performance Metrics" className="mb-3">
                            <PerformanceMetrics summary={summary} nodeTag={selectedNode} />
                        </AnalysisSection>
                        <AnalysisSection id="storage-per-database" label="Storage per Database" className="mb-3">
                            <StoragePerDatabase summary={summary} nodeTag={selectedNode} />
                        </AnalysisSection>
                        <AnalysisSection id="indexing-per-node" label="Indexing per Node" className="mb-3">
                            <IndexingPerNode summary={summary} nodeTag={selectedNode} />
                        </AnalysisSection>
                        <AnalysisSection id="ongoing-tasks" label="Ongoing Tasks">
                            <OngoingTasks summary={summary} nodeTag={selectedNode} />
                        </AnalysisSection>
                    </>
                )}

                {deferredContext === "database" &&
                    (deferredDatabase ? (
                        <DatabaseContextView
                            summary={summary}
                            database={deferredDatabase}
                            selectedNode={effectiveDatabaseNode}
                        />
                    ) : (
                        <EmptySet>No databases found in the package</EmptySet>
                    ))}
            </div>
        </div>
    );
}

function findScrollParent(element: HTMLElement | null): HTMLElement | null {
    let node = element?.parentElement ?? null;
    while (node) {
        const overflowY = window.getComputedStyle(node).overflowY;
        if ((overflowY === "auto" || overflowY === "scroll") && node.scrollHeight > node.clientHeight) {
            return node;
        }
        node = node.parentElement;
    }
    return null;
}

function defaultNode(summary: DebugPackageAnalysisSummary, nodeTags: string[]): string {
    const leader = nodeTags.find((tag) => summary.SummaryPerNode?.[tag]?.ClusterNodeInfo?.NodeState === "Leader");
    return leader ?? nodeTags[0] ?? null;
}

function collectDatabaseNodes(summary: DebugPackageAnalysisSummary, database: string | null): string[] {
    if (!database) {
        return [];
    }
    const nodes: string[] = [];
    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        const hasDb = (node.DatabasesOverview?.Items ?? []).some(
            (item) => item.Database === database && !item.Irrelevant
        );
        if (hasDb) {
            nodes.push(nodeTag);
        }
    });
    return nodes.sort();
}

function collectDatabaseNames(summary: DebugPackageAnalysisSummary): string[] {
    const names = new Set<string>();
    Object.values(summary.SummaryPerNode ?? {}).forEach((node) => {
        (node.DatabasesOverview?.Items ?? []).forEach((item) => {
            if (!item.Irrelevant) {
                names.add(item.Database);
            }
        });
    });
    return Array.from(names).sort();
}
