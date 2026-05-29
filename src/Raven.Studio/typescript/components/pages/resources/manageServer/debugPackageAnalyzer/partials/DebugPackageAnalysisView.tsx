import React, { useMemo, useState } from "react";
import PackageInfo from "./PackageInfo";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import { InputItem } from "components/models/common";
import { FlexGrow } from "components/common/FlexGrow";
import { EmptySet } from "components/common/EmptySet";
import AnalysisResults from "./AnalysisResults";
import ClusterOverview from "./ClusterOverview";
import { flattenIssues } from "./analyzerUtils";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type AnalysisContext = "cluster" | "node" | "database";

interface DebugPackageAnalysisViewProps {
    summary: DebugPackageAnalysisSummary;
    fileName: string;
    onReset: () => void;
}

export default function DebugPackageAnalysisView({ summary, fileName, onReset }: DebugPackageAnalysisViewProps) {
    const [context, setContext] = useState<AnalysisContext>("cluster");

    const contextItems: InputItem<AnalysisContext>[] = [
        { label: "Cluster", value: "cluster" },
        { label: "Node", value: "node" },
        { label: "Database", value: "database" },
    ];

    const issues = useMemo(() => flattenIssues(summary), [summary]);

    return (
        <div className="debug-package-analysis vstack gap-4">
            <div className="hstack gap-3 align-items-end flex-wrap">
                <MultiRadioToggle<AnalysisContext>
                    inputItems={contextItems}
                    selectedItem={context}
                    setSelectedItem={setContext}
                    label="Select analysis context"
                />
                <FlexGrow />
                <PackageInfo fileName={fileName} onReset={onReset} />
            </div>

            <AnalysisResults issues={issues} />

            {context === "cluster" ? (
                <ClusterOverview summary={summary} />
            ) : (
                <EmptySet>This analysis context is coming in a future iteration</EmptySet>
            )}
        </div>
    );
}
