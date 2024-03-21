import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import {
    RichPanel,
    RichPanelActions,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import useBoolean from "components/hooks/useBoolean";
import DatabaseCustomSorterTest from "components/pages/database/settings/customSorters/DatabaseCustomSorterTest";
import { Icon } from "components/common/Icon";
import React from "react";
import { UseAsyncReturn } from "react-async-hook";
import { Button, Collapse } from "reactstrap";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { useAppSelector } from "components/store";

interface DatabaseCustomSortersServerWideListProps {
    asyncGetSorters: UseAsyncReturn<Raven.Client.Documents.Queries.Sorting.SorterDefinition[], any[]>;
}

export default function DatabaseCustomSortersServerWideList({
    asyncGetSorters,
}: DatabaseCustomSortersServerWideListProps) {
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.hasDatabaseAdminAccess());

    const { value: isTestMode, toggle: toggleIsTestMode } = useBoolean(false);

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

                            {hasDatabaseAdminAccess && (
                                <RichPanelActions>
                                    <Button onClick={toggleIsTestMode}>
                                        <Icon icon="rocket" addon={isTestMode ? "cancel" : null} margin="m-0" />
                                    </Button>
                                </RichPanelActions>
                            )}
                        </RichPanelInfo>
                    </RichPanelHeader>
                    <Collapse isOpen={isTestMode}>
                        <DatabaseCustomSorterTest name={sorter.Name} />
                    </Collapse>
                </RichPanel>
            ))}
        </div>
    );
}
