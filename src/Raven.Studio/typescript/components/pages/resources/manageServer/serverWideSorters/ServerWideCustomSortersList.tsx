import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import React from "react";
import { AsyncStateStatus } from "react-async-hook";
import ServerWideCustomSortersListItem from "components/pages/resources/manageServer/serverWideSorters/ServerWideCustomSortersListItem";
import { CustomSorter } from "components/common/customSorters/useCustomSorters";

interface ServerWideCustomSortersListProps {
    sorters: CustomSorter[];
    fetchStatus: AsyncStateStatus;
    reload: () => void;
    takenNames: string[];
    remove: (idx: number) => void;
    markAsSaved: (idx: number, name: string) => void;
}

export default function ServerWideCustomSortersList(props: ServerWideCustomSortersListProps) {
    const { sorters, fetchStatus, reload, remove, takenNames, markAsSaved } = props;

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Unable to load custom sorters" refresh={reload} />;
    }

    if (sorters.length === 0) {
        return <EmptySet>No server-wide custom sorters have been defined</EmptySet>;
    }

    return sorters.map((sorter, idx) => (
        <ServerWideCustomSortersListItem
            key={sorter.id}
            initialSorter={sorter}
            takenNames={takenNames.filter((name) => name !== sorter.name)}
            remove={() => remove(idx)}
            markAsSaved={(name: string) => markAsSaved(idx, name)}
        />
    ));
}
