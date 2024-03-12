import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import { RichPanel, RichPanelHeader, RichPanelInfo, RichPanelName } from "components/common/RichPanel";
import React from "react";
import { UseAsyncReturn } from "react-async-hook";

interface CustomSortersReadOnlyListProps {
    asyncGetSorters: UseAsyncReturn<Raven.Client.Documents.Queries.Sorting.SorterDefinition[], any[]>;
}

export default function CustomSortersReadOnlyList({ asyncGetSorters }: CustomSortersReadOnlyListProps) {
    if (asyncGetSorters.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGetSorters.status === "error") {
        return <LoadError error="Unable to load custom sorters" refresh={asyncGetSorters.execute} />;
    }

    if (asyncGetSorters.result.length === 0) {
        return <EmptySet>No server-wide custom sorters have been defined</EmptySet>;
    }

    return (
        <div>
            {asyncGetSorters.result.map((sorter) => (
                <RichPanel key={sorter.Name} className="mt-3">
                    <RichPanelHeader>
                        <RichPanelInfo>
                            <RichPanelName>{sorter.Name}</RichPanelName>
                        </RichPanelInfo>
                    </RichPanelHeader>
                </RichPanel>
            ))}
        </div>
    );
}
