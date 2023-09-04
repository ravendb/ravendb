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
import DeleteCustomSorterConfirm from "components/common/customSorters/DeleteCustomSorterConfirm";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import React, { useState } from "react";
import { AsyncStateStatus, useAsyncCallback } from "react-async-hook";

interface ServerWideCustomSortersListProps {
    fetchStatus: AsyncStateStatus;
    sorters: Raven.Client.Documents.Queries.Sorting.SorterDefinition[];
    reload: () => void;
    isReadOnly?: boolean;
}

export default function ServerWideCustomSortersList({
    fetchStatus,
    sorters,
    reload,
    isReadOnly,
}: ServerWideCustomSortersListProps) {
    const { manageServerService } = useServices();

    const asyncDeleteSorter = useAsyncCallback(manageServerService.deleteServerWideCustomSorter, {
        onSuccess: reload,
    });

    const [nameToConfirmDelete, setNameToConfirmDelete] = useState<string>(null);

    const { appUrl } = useAppUrls();

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Unable to load custom sorters" refresh={reload} />;
    }

    if (sorters.length === 0) {
        return <EmptySet>No server-wide custom sorters have been defined</EmptySet>;
    }

    todo("Feature", "Damian", "Render react edit sorter");

    return (
        <div>
            {sorters.map((sorter) => (
                <RichPanel key={sorter.Name} className="mt-3">
                    <RichPanelHeader>
                        <RichPanelInfo>
                            <RichPanelName>{sorter.Name}</RichPanelName>
                        </RichPanelInfo>
                        {!isReadOnly && (
                            <RichPanelActions>
                                <a
                                    href={appUrl.forEditServerWideCustomSorter(sorter.Name)}
                                    className="btn btn-secondary"
                                >
                                    <Icon icon="edit" margin="m-0" />
                                </a>

                                {nameToConfirmDelete != null && (
                                    <DeleteCustomSorterConfirm
                                        name={nameToConfirmDelete}
                                        onConfirm={(name) => asyncDeleteSorter.execute(name)}
                                        toggle={() => setNameToConfirmDelete(null)}
                                        isServerWide
                                    />
                                )}
                                <ButtonWithSpinner
                                    color="danger"
                                    onClick={() => setNameToConfirmDelete(sorter.Name)}
                                    icon="trash"
                                    isSpinning={asyncDeleteSorter.status === "loading"}
                                />
                            </RichPanelActions>
                        )}
                    </RichPanelHeader>
                </RichPanel>
            ))}
        </div>
    );
}
