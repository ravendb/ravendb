import { Icon } from "components/common/Icon";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
} from "components/common/RichPanel";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import DatabaseCustomSorterTest from "components/pages/database/settings/customSorters/DatabaseCustomSorterTest";
import { useAppSelector } from "components/store";
import React from "react";
import Collapse from "react-bootstrap/Collapse";
import Button from "react-bootstrap/Button";

interface DatabaseCustomSortersServerWideListItemProps {
    sorter: Raven.Client.Documents.Queries.Sorting.SorterDefinition;
}

export default function DatabaseCustomSortersServerWideListItem({
    sorter,
}: DatabaseCustomSortersServerWideListItemProps) {
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { value: isTestMode, toggle: toggleIsTestMode } = useBoolean(false);

    return (
        <RichPanel className="mt-3">
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelName>{sorter.Name}</RichPanelName>

                    {hasDatabaseAdminAccess && (
                        <RichPanelActions>
                            <Button
                                variant="secondary"
                                onClick={toggleIsTestMode}
                                title={isTestMode ? "Exit test mode" : "Test custom sorter"}
                            >
                                <Icon icon="rocket" addon={isTestMode ? "cancel" : null} margin="m-0" />
                            </Button>
                        </RichPanelActions>
                    )}
                </RichPanelInfo>
            </RichPanelHeader>
            <Collapse in={isTestMode}>
                <div>
                    <DatabaseCustomSorterTest name={sorter.Name} />
                </div>
            </Collapse>
        </RichPanel>
    );
}
