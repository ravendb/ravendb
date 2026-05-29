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
import StoragePerDatabase from "./StoragePerDatabase";
import IndexingPerNode from "./IndexingPerNode";
import OngoingTasks from "./OngoingTasks";
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
    const [context, setContext] = useState<AnalysisContext>("cluster");
    const [selectedNode, setSelectedNode] = useState<string>(() => defaultNode(summary, nodeTags));

    const allIssues = useMemo(() => flattenIssues(summary), [summary]);

    const contextIssues = useMemo(() => {
        if (context === "node") {
            return allIssues.filter((issue) => issue.nodeTag === selectedNode);
        }
        return allIssues;
    }, [allIssues, context, selectedNode]);

    const contextItems: InputItem<AnalysisContext>[] = [
        { label: "Cluster", value: "cluster", count: allIssues.length },
        { label: "Node", value: "node", count: allIssues.filter((i) => i.nodeTag === selectedNode).length },
        { label: "Database", value: "database", count: allIssues.filter((i) => i.scope === "database").length },
    ];

    const nodeOptions: SelectOption<string>[] = nodeTags.map((tag) => ({ value: tag, label: `Node ${tag}` }));

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
                    <div className="d-flex gap-4 flex-wrap">
                        <StoragePerDatabase summary={summary} nodeTag={selectedNode} />
                        <IndexingPerNode summary={summary} nodeTag={selectedNode} />
                    </div>
                    <OngoingTasks summary={summary} nodeTag={selectedNode} />
                </>
            )}
            {context === "database" && <EmptySet>The Database context is coming in a future iteration</EmptySet>}
        </div>
    );
}

function defaultNode(summary: DebugPackageAnalysisSummary, nodeTags: string[]): string {
    const leader = nodeTags.find((tag) => summary.SummaryPerNode?.[tag]?.ClusterNodeInfo?.NodeState === "Leader");
    return leader ?? nodeTags[0] ?? null;
}
