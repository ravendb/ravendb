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
import { Button, UncontrolledPopover } from "reactstrap";
import { Icon } from "components/common/Icon";
import {
    DocumentRevisionsConfig,
    DocumentRevisionsConfigName,
    documentRevisionsActions,
    documentRevisionsConfigNames,
} from "./store/documentRevisionsSlice";
import { documentRevisionsSelectors } from "./store/documentRevisionsSliceSelectors";
import classNames from "classnames";
import { useAppDispatch, useAppSelector } from "components/store";
import { Checkbox } from "components/common/Checkbox";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import generalUtils from "common/generalUtils";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

interface DocumentRevisionsConfigPanelProps {
    config: DocumentRevisionsConfig;
    onToggle: () => void;
    onEdit: () => void;
    onDelete?: () => void;
}

export default function DocumentRevisionsConfigPanel(props: DocumentRevisionsConfigPanelProps) {
    const { config, onDelete, onToggle, onEdit } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const dispatch = useAppDispatch();
    const { reportEvent } = useEventsCollector();

    const originalConfig = useAppSelector(documentRevisionsSelectors.originalConfig(config.Name));
    const isSelected = useAppSelector(documentRevisionsSelectors.isSelectedConfigName(config.Name));

    const isModified = !_.isEqual(originalConfig, config);

    const isDeleteOnUpdateVisible =
        (config.MinimumRevisionsToKeep || config.MinimumRevisionAgeToKeep) &&
        config.MaximumRevisionsToDeleteUponDocumentUpdate;

    const isDetailsVisible = config.PurgeOnDelete || config.MinimumRevisionsToKeep || config.MinimumRevisionAgeToKeep;

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
                        {hasDatabaseAdminAccess && (
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
                        <DefaultConfigInfoIcon name={config.Name} />
                        {hasDatabaseAdminAccess && (
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

                                <Button color="secondary" onClick={onEdit} title="Edit this revision configuration">
                                    <Icon icon="edit" margin="m-0" />
                                </Button>
                                {onDelete && (
                                    <Button
                                        color="danger"
                                        onClick={() => {
                                            reportEvent("revisions", "remove");
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
                                {config.MinimumRevisionsToKeep} revisions
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
                                {config.MaximumRevisionsToDeleteUponDocumentUpdate} revisions
                            </RichPanelDetailItem>
                        )}
                    </RichPanelDetails>
                )}
            </div>
        </RichPanel>
    );
}

function DefaultConfigInfoIcon({ name }: { name: DocumentRevisionsConfigName }) {
    const isDefault =
        name === documentRevisionsConfigNames.defaultDocument || name === documentRevisionsConfigNames.defaultConflicts;

    if (!isDefault) {
        return null;
    }

    const id = "info-" + name.split(" ").join("-");

    return (
        <>
            <UncontrolledPopover target={id} placement="left" trigger="hover">
                <ul className="margin-top margin-top-xs">
                    {name === documentRevisionsConfigNames.defaultConflicts ? (
                        <>
                            <li>
                                This is the default revision configuration for
                                <strong> conflicting documents only</strong>.
                            </li>
                            <li>When enabled, a revision is created for each conflicting item.</li>
                            <li>A revision is also created for the conflict resolution document.</li>
                            <li>
                                When Document Defaults or a collection-specific configuration is defined, they{" "}
                                <strong>override</strong> the Conflicting Document Defaults.
                            </li>
                        </>
                    ) : (
                        <>
                            <li>
                                This is the default revision configuration for all
                                <strong> non-conflicting documents</strong>.
                            </li>
                            <li>When enabled, a revision is created for all non-conflicting documents.</li>
                            <li>
                                When a collection specific configuration is defined, it <strong>overrides</strong> these
                                defaults.
                            </li>
                        </>
                    )}
                </ul>
            </UncontrolledPopover>
            <Icon id={id} icon="info" />
        </>
    );
}
