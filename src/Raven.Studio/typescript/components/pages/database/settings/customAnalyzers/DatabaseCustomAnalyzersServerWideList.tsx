import { UseAsyncReturn } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { EmptySet } from "components/common/EmptySet";
import React from "react";
import { RichPanel, RichPanelHeader, RichPanelInfo, RichPanelName } from "components/common/RichPanel";

interface DatabaseCustomAnalyzersServerWideListProps {
    asyncGetAnalyzers: UseAsyncReturn<Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition[], any[]>;
}

export default function DatabaseCustomAnalyzersServerWideList({
    asyncGetAnalyzers,
}: DatabaseCustomAnalyzersServerWideListProps) {
    if (asyncGetAnalyzers.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGetAnalyzers.status === "error") {
        return <LoadError error="Unable to load custom analyzers" refresh={asyncGetAnalyzers.execute} />;
    }

    if (asyncGetAnalyzers.result.length === 0) {
        return <EmptySet>No server-wide custom analyzers have been defined</EmptySet>;
    }

    return (
        <div>
            {asyncGetAnalyzers.result.map((analyzer) => (
                <RichPanel key={analyzer.Name} className="mt-3">
                    <RichPanelHeader>
                        <RichPanelInfo>
                            <RichPanelName>{analyzer.Name}</RichPanelName>
                        </RichPanelInfo>
                    </RichPanelHeader>
                </RichPanel>
            ))}
        </div>
    );
}
