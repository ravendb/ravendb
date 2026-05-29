import React, { useMemo, useState } from "react";
import PackageInfo from "./PackageInfo";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import { InputItem } from "components/models/common";
import { FlexGrow } from "components/common/FlexGrow";
import { EmptySet } from "components/common/EmptySet";
import Select, { SelectOption } from "components/common/select/Select";
import AnalysisResults from "./AnalysisResults";
import ClusterOverview from "./ClusterOverview";
import DatabasesOverview from "./DatabasesOverview";
import NodeOverview from "./NodeOverview";
import PerformanceMetrics from "./PerformanceMetrics";
import StoragePerDatabase from "./StoragePerDatabase";
import IndexingPerNode from "./IndexingPerNode";
import OngoingTasks from "./OngoingTasks";
import DatabaseContextView from "./DatabaseContextView";
import { flattenIssues } from "./analyzerUtils";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type AnalysisContext = "cluster" | "node" | "database";

interface DebugPackageAnalysisViewProps {
    summary: DebugPackageAnalysisSummary;
    fileName: string;
    onReset: () => void;
}

export default function DebugPackageAnalysisView({ summary, fileName, onReset }: DebugPackageAnalysisViewProps) {
    const nodeTags = useMemo(() => Object.keys(summary.SummaryPerNode ?? {}).sort(), [summary]);
    const databaseNames = useMemo(() => collectDatabaseNames(summary), [summary]);
    const [context, setContext] = useState<AnalysisContext>("cluster");
    const [selectedNode, setSelectedNode] = useState<string>(() => defaultNode(summary, nodeTags));
    const [selectedDatabase, setSelectedDatabase] = useState<string>(() => databaseNames[0] ?? null);

    const allIssues = useMemo(() => flattenIssues(summary), [summary]);

    const contextIssues = useMemo(() => {
        if (context === "node") {
            return allIssues.filter((issue) => issue.nodeTag === selectedNode);
        }
        if (context === "database") {
            return allIssues.filter((issue) => issue.scope === "database" && issue.database === selectedDatabase);
        }
        return allIssues;
    }, [allIssues, context, selectedNode, selectedDatabase]);

    const contextItems: InputItem<AnalysisContext>[] = [
        { label: "Cluster", value: "cluster", count: allIssues.length },
        { label: "Node", value: "node", count: allIssues.filter((i) => i.nodeTag === selectedNode).length },
        {
            label: "Database",
            value: "database",
            count: allIssues.filter((i) => i.scope === "database" && i.database === selectedDatabase).length,
        },
    ];

    const nodeOptions: SelectOption<string>[] = nodeTags.map((tag) => ({ value: tag, label: `Node ${tag}` }));
    const databaseOptions: SelectOption<string>[] = databaseNames.map((name) => ({ value: name, label: name }));

    return (
        <div className="debug-package-analysis vstack gap-4">
            <div className="hstack gap-3 align-items-end flex-wrap">
                <MultiRadioToggle<AnalysisContext>
                    inputItems={contextItems}
                    selectedItem={context}
                    setSelectedItem={setContext}
                    label="Select analysis context"
                />
                {context === "node" && nodeTags.length > 0 && (
                    <div className="node-select">
                        <div className="small-label ms-1 mb-1">Select node</div>
                        <Select
                            options={nodeOptions}
                            value={nodeOptions.find((o) => o.value === selectedNode)}
                            onChange={(option) => option && setSelectedNode(option.value)}
                            isSearchable={false}
                            isRoundedPill
                        />
                    </div>
                )}
                {context === "database" && databaseNames.length > 0 && (
                    <div className="node-select">
                        <div className="small-label ms-1 mb-1">Select database</div>
                        <Select
                            options={databaseOptions}
                            value={databaseOptions.find((o) => o.value === selectedDatabase)}
                            onChange={(option) => option && setSelectedDatabase(option.value)}
                            isSearchable
                            isRoundedPill
                        />
                    </div>
                )}
                <FlexGrow />
                <PackageInfo fileName={fileName} onReset={onReset} />
            </div>

            <AnalysisResults issues={contextIssues} />

            {context === "cluster" && (
                <>
                    <ClusterOverview summary={summary} />
                    <DatabasesOverview summary={summary} />
                    <div className="d-flex gap-4 flex-wrap">
                        <StoragePerDatabase summary={summary} />
                        <IndexingPerNode summary={summary} />
                    </div>
                    <OngoingTasks summary={summary} />
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
                (selectedDatabase ? (
                    <DatabaseContextView summary={summary} database={selectedDatabase} />
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
