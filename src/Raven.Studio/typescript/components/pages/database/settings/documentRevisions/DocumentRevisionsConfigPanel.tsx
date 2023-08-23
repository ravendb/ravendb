import React from "react";
import {
    RichPanel,
    RichPanelStatus,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
    RichPanelDetailItem,
} from "components/common/RichPanel";
import { Button } from "reactstrap";
import { Icon } from "components/common/Icon";
import {
    DocumentRevisionsConfig,
    documentRevisionsActions,
    documentRevisionsSelectors,
} from "./store/documentRevisionsSlice";
import classNames from "classnames";
import { useAppDispatch, useAppSelector } from "components/store";
import { Checkbox } from "components/common/Checkbox";

interface DocumentRevisionsConfigPanelProps {
    config: DocumentRevisionsConfig;
    onDelete?: () => void;
    onToggle: () => void;
    onOnEdit: () => void;
}

export default function DocumentRevisionsConfigPanel(props: DocumentRevisionsConfigPanelProps) {
    const { config, onDelete, onToggle, onOnEdit } = props;

    const dispatch = useAppDispatch();
    const originalConfigs = useAppSelector(documentRevisionsSelectors.originalConfigs);
    const isSelected = useAppSelector(documentRevisionsSelectors.isSelectedConfigName(config?.Name));

    if (!config) {
        return null;
    }

    const isModified = !_.isEqual(
        originalConfigs.find((x) => x.Name === config.Name),
        config
    );

    const isDeleteOnUpdateVisible =
        (config.MinimumRevisionsToKeep || config.MinimumRevisionAgeToKeep) &&
        config.MaximumRevisionsToDeleteUponDocumentUpdate;

    const isDetailsVisible = config.PurgeOnDelete || config.MinimumRevisionsToKeep || config.MinimumRevisionAgeToKeep;

    // TODO kalczur tooltip for defaults
    // TODO kalczur format retention time

    return (
        <RichPanel className="flex-row with-status">
            <RichPanelStatus color={config.Disabled ? "warning" : "success"}>
                <div className={classNames({ "my-1": !isDetailsVisible })}>
                    {config.Disabled ? "Disabled" : "Enabled"}
                </div>
            </RichPanelStatus>
            <div className="flex-grow-1">
                <RichPanelHeader className={classNames({ "h-100": !isDetailsVisible })}>
                    <RichPanelInfo>
                        <RichPanelSelect>
                            <Checkbox
                                selected={isSelected}
                                toggleSelection={() =>
                                    dispatch(documentRevisionsActions.toggleSelectedConfigName(config.Name))
                                }
                            />
                        </RichPanelSelect>
                        <RichPanelName>
                            {config.Name}
                            {isModified && <span className="text-warning ms-1">*</span>}
                        </RichPanelName>
                    </RichPanelInfo>
                    <RichPanelActions>
                        <Button color={config.Disabled ? "success" : "secondary"} onClick={onToggle}>
                            <Icon icon={config.Disabled ? "start" : "disable"} />
                            {config.Disabled ? "Enable" : "Disable"}
                        </Button>
                        <Button color="secondary" onClick={onOnEdit}>
                            <Icon icon="edit" margin="m-0" />
                        </Button>
                        {onDelete && (
                            <Button color="danger" onClick={onDelete}>
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        )}
                    </RichPanelActions>
                </RichPanelHeader>
                {isDetailsVisible && (
                    <RichPanelDetails>
                        {config.PurgeOnDelete && (
                            <RichPanelDetailItem>
                                <Icon icon="empty-set" />
                                Purge revisions on document delete
                            </RichPanelDetailItem>
                        )}
                        {config.MinimumRevisionsToKeep && (
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="documents" />
                                        Keep
                                    </>
                                }
                            >
                                {config.MinimumRevisionsToKeep}
                            </RichPanelDetailItem>
                        )}
                        {config.MinimumRevisionAgeToKeep && (
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="clock" />
                                        Retention time
                                    </>
                                }
                            >
                                {config.MinimumRevisionAgeToKeep}
                            </RichPanelDetailItem>
                        )}
                        {isDeleteOnUpdateVisible && (
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="trash" />
                                        Delete on update
                                    </>
                                }
                            >
                                {config.MaximumRevisionsToDeleteUponDocumentUpdate}
                            </RichPanelDetailItem>
                        )}
                    </RichPanelDetails>
                )}
            </div>
        </RichPanel>
    );
}
