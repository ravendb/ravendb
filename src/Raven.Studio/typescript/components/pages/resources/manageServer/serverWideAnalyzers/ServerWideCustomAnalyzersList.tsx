import { todo } from "common/developmentHelper";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
} from "components/common/RichPanel";
import DeleteCustomAnalyzerConfirm from "components/common/customAnalyzers/DeleteCustomAnalyzerConfirm";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import React, { useState } from "react";
import { AsyncStateStatus, useAsyncCallback } from "react-async-hook";

interface ServerWideCustomAnalyzersListProps {
    fetchStatus: AsyncStateStatus;
    analyzers: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition[];
    reload: () => void;
    isReadOnly?: boolean;
}

export default function ServerWideCustomAnalyzersList({
    fetchStatus,
    analyzers,
    reload,
    isReadOnly,
}: ServerWideCustomAnalyzersListProps) {
    const { manageServerService } = useServices();

    const asyncDeleteAnalyzer = useAsyncCallback(manageServerService.deleteServerWideCustomAnalyzer, {
        onSuccess: reload,
    });

    const [nameToConfirmDelete, setNameToConfirmDelete] = useState<string>(null);

    const { appUrl } = useAppUrls();

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Unable to load custom analyzers" refresh={reload} />;
    }

    if (analyzers.length === 0) {
        return <EmptySet>No server-wide custom analyzers have been defined</EmptySet>;
    }

    todo("Feature", "Damian", "Render react edit analyzer");

    return (
        <div>
            {analyzers.map((analyzer) => (
                <RichPanel key={analyzer.Name} className="mt-3">
                    <RichPanelHeader>
                        <RichPanelInfo>
                            <RichPanelName>{analyzer.Name}</RichPanelName>
                        </RichPanelInfo>
                        {!isReadOnly && (
                            <RichPanelActions>
                                <a
                                    href={appUrl.forEditServerWideCustomAnalyzer(analyzer.Name)}
                                    className="btn btn-secondary"
                                >
                                    <Icon icon="edit" margin="m-0" />
                                </a>

                                {nameToConfirmDelete != null && (
                                    <DeleteCustomAnalyzerConfirm
                                        name={nameToConfirmDelete}
                                        onConfirm={(name) => asyncDeleteAnalyzer.execute(name)}
                                        toggle={() => setNameToConfirmDelete(null)}
                                        isServerWide
                                    />
                                )}
                                <ButtonWithSpinner
                                    color="danger"
                                    onClick={() => setNameToConfirmDelete(analyzer.Name)}
                                    icon="trash"
                                    isSpinning={asyncDeleteAnalyzer.status === "loading"}
                                />
                            </RichPanelActions>
                        )}
                    </RichPanelHeader>
                </RichPanel>
            ))}
        </div>
    );
}
