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
import { useEventsCollector } from "components/hooks/useEventsCollector";
import generalUtils from "common/generalUtils";

interface DocumentRevisionsConfigPanelProps {
    config: DocumentRevisionsConfig;
    isDatabaseAdmin: boolean;
    onToggle: () => void;
    onOnEdit: () => void;
    onDelete?: () => void;
}

export default function DocumentRevisionsConfigPanel(props: DocumentRevisionsConfigPanelProps) {
    const { config, isDatabaseAdmin, onDelete, onToggle, onOnEdit } = props;

    const dispatch = useAppDispatch();
    const { reportEvent } = useEventsCollector();

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

    const formattedMinimumRevisionAgeToKeep = config.MinimumRevisionAgeToKeep
        ? generalUtils.formatTimeSpan(generalUtils.timeSpanToSeconds(config.MinimumRevisionAgeToKeep) * 1000, true)
        : null;

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
                        {isDatabaseAdmin && (
                            <RichPanelSelect>
                                <Checkbox
                                    selected={isSelected}
                                    toggleSelection={() =>
                                        dispatch(documentRevisionsActions.toggleSelectedConfigName(config.Name))
                                    }
                                />
                            </RichPanelSelect>
                        )}
                        <RichPanelName>
                            {config.Name}
                            {isModified && <span className="text-warning ms-1">*</span>}
                        </RichPanelName>
                    </RichPanelInfo>
                    <RichPanelActions>
                        {isDatabaseAdmin && (
                            <>
                                <Button
                                    color={config.Disabled ? "success" : "secondary"}
                                    onClick={onToggle}
                                    title={`Click to ${
                                        config.Disabled ? "enable" : "disable"
                                    } this revision configuration`}
                                >
                                    <Icon icon={config.Disabled ? "start" : "disable"} />
                                    {config.Disabled ? "Enable" : "Disable"}
                                </Button>

                                <Button color="secondary" onClick={onOnEdit} title="Edit this revision configuration">
                                    <Icon icon="edit" margin="m-0" />
                                </Button>
                                {onDelete && (
                                    <Button
                                        color="danger"
                                        onClick={() => {
                                            reportEvent("revisions", "create");
                                            onDelete();
                                        }}
                                        title="Delete this revision configuration"
                                    >
                                        <Icon icon="trash" margin="m-0" />
                                    </Button>
                                )}
                            </>
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
                        {formattedMinimumRevisionAgeToKeep && (
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="clock" />
                                        Retention time
                                    </>
                                }
                            >
                                {formattedMinimumRevisionAgeToKeep}
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
