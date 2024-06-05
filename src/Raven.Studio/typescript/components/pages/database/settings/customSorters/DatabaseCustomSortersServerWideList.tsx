import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import React from "react";
import { UseAsyncReturn } from "react-async-hook";
import DatabaseCustomSortersServerWideListItem from "components/pages/database/settings/customSorters/DatabaseCustomSortersServerWideListItem";

interface DatabaseCustomSortersServerWideListProps {
    asyncGetSorters: UseAsyncReturn<Raven.Client.Documents.Queries.Sorting.SorterDefinition[], any[]>;
}

export default function DatabaseCustomSortersServerWideList({
    asyncGetSorters,
}: DatabaseCustomSortersServerWideListProps) {
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
                <DatabaseCustomSortersServerWideListItem key={sorter.Name} sorter={sorter} />
            ))}
        </div>
    );
}
