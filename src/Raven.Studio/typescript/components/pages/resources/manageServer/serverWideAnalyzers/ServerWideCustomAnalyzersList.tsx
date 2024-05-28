import { CustomAnalyzer } from "components/common/customAnalyzers/useCustomAnalyzers";
import { AsyncStateStatus } from "react-async-hook";
import React from "react";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { EmptySet } from "components/common/EmptySet";
import ServerWideCustomAnalyzersListItem from "components/pages/resources/manageServer/serverWideAnalyzers/ServerWideCustomAnalyzersListItem";

interface ServerWideCustomAnalyzersListProps {
    analyzers: CustomAnalyzer[];
    fetchStatus: AsyncStateStatus;
    reload: () => void;
    remove: (idx: number) => void;
}

export default function ServerWideCustomAnalyzersList(props: ServerWideCustomAnalyzersListProps) {
    const { analyzers, fetchStatus, reload, remove } = props;

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Unable to load custom analyzers" refresh={reload} />;
    }

    if (analyzers.length === 0) {
        return <EmptySet>No server-wide custom analyzers have been defined</EmptySet>;
    }

    return analyzers.map((analyzer, idx) => (
        <ServerWideCustomAnalyzersListItem key={analyzer.id} initialAnalyzer={analyzer} remove={() => remove(idx)} />
    ));
}
