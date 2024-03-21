import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import DatabaseCustomSortersListItem from "components/pages/database/settings/customSorters/DatabaseCustomSortersListItem";
import { CustomSorterFormData } from "components/common/customSorters/editCustomSorterValidation";
import React from "react";
import { AsyncStateStatus } from "react-async-hook";

interface DatabaseCustomSortersListProps {
    sorters: CustomSorterFormData[];
    fetchStatus: AsyncStateStatus;
    reload: () => void;
    serverWideSorterNames: string[];
    remove: (idx: number) => void;
}

export default function DatabaseCustomSortersList(props: DatabaseCustomSortersListProps) {
    const { sorters, fetchStatus, reload, remove, serverWideSorterNames } = props;

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Unable to load custom sorters" refresh={reload} />;
    }

    if (sorters.length === 0) {
        return <EmptySet>No custom sorters have been defined</EmptySet>;
    }

    return (
        <div>
            {sorters.map((sorter, idx) => (
                <DatabaseCustomSortersListItem
                    key={sorter.name + idx}
                    initialSorter={sorter}
                    serverWideSorterNames={serverWideSorterNames}
                    remove={() => remove(idx)}
                />
            ))}
        </div>
    );
}
