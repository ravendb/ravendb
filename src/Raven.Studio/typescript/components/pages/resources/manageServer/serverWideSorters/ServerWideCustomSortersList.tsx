import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import React from "react";
import { AsyncStateStatus } from "react-async-hook";
import { CustomSorterFormData } from "components/common/customSorters/editCustomSorterValidation";
import ServerWideCustomSortersListItem from "components/pages/resources/manageServer/serverWideSorters/ServerWideCustomSortersListItem";

interface ServerWideCustomSortersListProps {
    sorters: CustomSorterFormData[];
    fetchStatus: AsyncStateStatus;
    reload: () => void;
    remove: (idx: number) => void;
}

export default function ServerWideCustomSortersList(props: ServerWideCustomSortersListProps) {
    const { sorters, fetchStatus, reload, remove } = props;

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Unable to load custom sorters" refresh={reload} />;
    }

    if (sorters.length === 0) {
        return <EmptySet>No server-wide custom sorters have been defined</EmptySet>;
    }

    return (
        <div>
            {sorters.map((sorter, idx) => (
                <ServerWideCustomSortersListItem
                    key={sorter.name + idx}
                    initialSorter={sorter}
                    remove={() => remove(idx)}
                />
            ))}
        </div>
    );
}
