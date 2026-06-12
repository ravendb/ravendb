import React, { useDeferredValue, useMemo, useState } from "react";
import classNames from "classnames";
import PackageInfo from "./PackageInfo";
import { Icon } from "components/common/Icon";
import { EmptySet } from "components/common/EmptySet";
import IconName from "typings/server/icons";
import AnalysisResults from "./AnalysisResults";
import AnalysisErrors from "./AnalysisErrors";
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

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type AnalysisContext = "cluster" | "node" | "database";

interface DebugPackageAnalysisViewProps {
    summary: DebugPackageAnalysisSummary;
    fileName: string;
    onReset: () => void;
}

interface ContextItem {
    value: AnalysisContext;
    label: string;
    icon: IconName;
    count: number;
}

export default function DebugPackageAnalysisView({ summary, fileName, onReset }: DebugPackageAnalysisViewProps) {
    const nodeTags = useMemo(() => Object.keys(summary.SummaryPerNode ?? {}).sort(), [summary]);
    const databaseNames = useMemo(() => collectDatabaseNames(summary), [summary]);
    const [context, setContext] = useState<AnalysisContext>("cluster");
    const [selectedNode, setSelectedNode] = useState<string>(() => defaultNode(summary, nodeTags));
    const [selectedDatabase, setSelectedDatabase] = useState<string>(() => databaseNames[0] ?? null);
    const deferredDatabase = useDeferredValue(selectedDatabase);
    const [selectedDatabaseNode, setSelectedDatabaseNode] = useState<string>(() => defaultNode(summary, nodeTags));
    const databaseNodes = useMemo(() => collectDatabaseNodes(summary, deferredDatabase), [summary, deferredDatabase]);
    const databaseOptions = useMemo(
        () => databaseNames.map((name): SelectOption<string> => ({ value: name, label: name })),
        [databaseNames]
    );

    const allIssues = useMemo(() => flattenIssues(summary), [summary]);

    const contextIssues = useMemo(() => {
        if (context === "node") {
            return allIssues.filter((issue) => issue.nodeTags.includes(selectedNode));
        }
        if (context === "database") {
            return allIssues.filter((issue) => issue.scope === "database" && issue.database === deferredDatabase);
        }
        return allIssues;
    }, [allIssues, context, selectedNode, deferredDatabase]);

    const contextItems = useMemo<ContextItem[]>(
        () => [
            { label: "Cluster", value: "cluster", icon: "cluster", count: allIssues.length },
            {
                label: "Node",
                value: "node",
                icon: "node",
                count: allIssues.filter((i) => i.nodeTags.includes(selectedNode)).length,
            },
            {
                label: "Database",
                value: "database",
                icon: "database",
                count: allIssues.filter((i) => i.scope === "database" && i.database === deferredDatabase).length,
            },
        ],
        [allIssues, selectedNode, deferredDatabase]
    );

    return (
        <div className="debug-package-analysis vstack gap-4">
            <div className="d-flex align-items-center justify-content-between gap-4 flex-wrap">
                <div className="d-flex align-items-center gap-3 flex-wrap">
                    <ContextToggle items={contextItems} selected={context} onSelect={setContext} />
                    {context === "node" && nodeTags.length > 0 && (
                        <div className="node-select">
                            <div className="context-toggle-label mb-1">Select node</div>
                            <Select<SelectOption<string>>
                                options={nodeTags.map(
                                    (tag): SelectOption<string> => ({ value: tag, label: `Node ${tag}` })
                                )}
                                value={{ value: selectedNode, label: `Node ${selectedNode}` }}
                                onChange={(option) => option && setSelectedNode(option.value)}
                                isSearchable={false}
                            />
                        </div>
                    )}
                    {context === "database" && databaseNames.length > 0 && (
                        <div className="node-select">
                            <div className="context-toggle-label mb-1">Select database</div>
                            <Select<SelectOption<string>>
                                options={databaseOptions}
                                value={selectedDatabase ? { value: selectedDatabase, label: selectedDatabase } : null}
                                onChange={(option) => option && setSelectedDatabase(option.value)}
                                isSearchable={false}
                                isLoading={deferredDatabase !== selectedDatabase}
                            />
                        </div>
                    )}
                    {context === "database" && nodeTags.length > 1 && (
                        <PopoverWithHoverWrapper
                            message={databaseNodes.length <= 1 ? "This database has data on one node only" : null}
                            placement="top"
                            overlayProps={{
                                popperConfig: {
                                    modifiers: [{ name: "offset", options: { offset: [0, -16] } }],
                                },
                            }}
                            inline={false}
                        >
                            <div className="node-select">
                                <div className="context-toggle-label mb-1">Select node</div>
                                <Select<SelectOption<string>>
                                    options={nodeTags.map(
                                        (tag): SelectOption<string> => ({ value: tag, label: `Node ${tag}` })
                                    )}
                                    value={{ value: selectedDatabaseNode, label: `Node ${selectedDatabaseNode}` }}
                                    onChange={(option) => option && setSelectedDatabaseNode(option.value)}
                                    isSearchable={false}
                                    isDisabled={databaseNodes.length <= 1}
                                />
                            </div>
                        </PopoverWithHoverWrapper>
                    )}
                </div>
                <PackageInfo fileName={fileName} onReset={onReset} />
            </div>

            <AnalysisErrors summary={summary} />

            <AnalysisResults issues={contextIssues} />

            {context === "cluster" && (
                <>
                    <ClusterOverview summary={summary} />
                    <ResourceUsage summary={summary} />
                    <DatabasesOverview summary={summary} />
                    <div className="d-flex gap-4 flex-wrap">
                        <StoragePerDatabase summary={summary} />
                        <IndexingPerNode summary={summary} />
                    </div>
                    <OngoingTasks summary={summary} />
                    <ClusterRaftDebug summary={summary} />
                    <ClusterObserverDecisions summary={summary} />
                </>
            )}
            {context === "node" && selectedNode && (
                <>
                    <NodeOverview summary={summary} nodeTag={selectedNode} />
                    <PerformanceMetrics summary={summary} nodeTag={selectedNode} />
                    <div className="d-flex gap-4 flex-wrap">
                        <StoragePerDatabase summary={summary} nodeTag={selectedNode} />
                        <IndexingPerNode summary={summary} nodeTag={selectedNode} />
                    </div>
                    <OngoingTasks summary={summary} nodeTag={selectedNode} />
                </>
            )}
            {context === "database" &&
                (deferredDatabase ? (
                    <DatabaseContextView
                        summary={summary}
                        database={deferredDatabase}
                        selectedNode={selectedDatabaseNode}
                    />
                ) : (
                    <EmptySet>No databases found in the package</EmptySet>
                ))}
        </div>
    );
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

interface ContextToggleProps {
    items: ContextItem[];
    selected: AnalysisContext;
    onSelect: (value: AnalysisContext) => void;
}

function ContextToggle({ items, selected, onSelect }: ContextToggleProps) {
    return (
        <div className="context-toggle">
            <div className="context-toggle-label mb-1">Select analysis context</div>
            <div className="context-toggle-container">
                {items.map((item) => (
                    <button
                        key={item.value}
                        type="button"
                        className={classNames("context-toggle-btn", { active: selected === item.value })}
                        onClick={() => onSelect(item.value)}
                    >
                        <Icon icon={item.icon} margin="m-0" />
                        <span>{item.label}</span>
                        <span className="context-toggle-badge">{item.count}</span>
                    </button>
                ))}
            </div>
        </div>
    );
}
