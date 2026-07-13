import React from "react";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import Spinner from "react-bootstrap/Spinner";
import appUrl from "common/appUrl";
import useBoolean from "hooks/useBoolean";
import { Checkbox } from "components/common/Checkbox";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
    RichPanelDetailItem,
} from "components/common/RichPanel";
import { OngoingTaskOperationConfirmType } from "components/pages/database/tasks/shared/OngoingTaskOperationConfirm";
import { ServerWideTaskInfo, ServerWideTaskSharedInfo } from "../serverWideTaskModels";

interface ServerWideTaskPanelProps {
    task: ServerWideTaskInfo;
    isSelected: (taskId: number) => boolean;
    toggleSelection: (checked: boolean, task: ServerWideTaskSharedInfo) => void;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, tasks: ServerWideTaskSharedInfo[]) => void;
    isDeleting: (taskName: string) => boolean;
    isTogglingState: (taskName: string) => boolean;
}

export default function ServerWideTaskPanel(props: ServerWideTaskPanelProps) {
    const { task, isSelected, toggleSelection, onTaskOperation, isDeleting, isTogglingState } = props;

    const { value: detailsVisible, toggle: toggleDetails } = useBoolean(false);

    const editUrl =
        task.taskType === "Backup"
            ? appUrl.forEditServerWideBackup(task.taskName)
            : appUrl.forEditServerWideExternalReplication(task.taskName);

    const togglingState = isTogglingState(task.taskName);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelSelect>
                        <Checkbox
                            selected={isSelected(task.taskId)}
                            toggleSelection={(e) => toggleSelection(e.currentTarget.checked, task)}
                        />
                    </RichPanelSelect>
                    <RichPanelName>
                        <a href={editUrl} title={"Task name: " + task.taskName}>
                            {task.taskName}
                        </a>
                    </RichPanelName>
                </RichPanelInfo>

                <RichPanelActions>
                    {task.responsibleNodeTag && (
                        <div title="Cluster node that is responsible for this task">
                            <Icon icon="cluster-node" />
                            {task.responsibleNodeTag}
                        </div>
                    )}
                    <Dropdown>
                        <Dropdown.Toggle
                            disabled={togglingState}
                            variant={task.taskState === "Disabled" ? "warning" : "secondary"}
                        >
                            {togglingState && <Spinner size="sm" />} {task.taskState}
                        </Dropdown.Toggle>
                        <Dropdown.Menu>
                            <Dropdown.Item onClick={() => onTaskOperation("enable", [task])}>
                                <Icon icon="play" color="success" /> Enable
                            </Dropdown.Item>
                            <Dropdown.Item onClick={() => onTaskOperation("disable", [task])}>
                                <Icon icon="stop" color="danger" /> Disable
                            </Dropdown.Item>
                        </Dropdown.Menu>
                    </Dropdown>
                    <ButtonGroup>
                        <Button variant="secondary" onClick={toggleDetails} title="Click for details">
                            <Icon icon={detailsVisible ? "fold" : "unfold"} margin="m-0" />
                        </Button>
                        <Button variant="secondary" href={editUrl} title="Edit task">
                            <Icon icon="edit" margin="m-0" />
                        </Button>
                        <ButtonWithSpinner
                            variant="danger"
                            isSpinning={isDeleting(task.taskName)}
                            onClick={() => onTaskOperation("delete", [task])}
                            title="Delete task"
                            spinnerMargin="m-0"
                            icon={{ icon: "trash", margin: "m-0" }}
                        />
                    </ButtonGroup>
                </RichPanelActions>
            </RichPanelHeader>

            {detailsVisible && (
                <RichPanelDetails>
                    {task.excludedDatabases.length > 0 && (
                        <RichPanelDetailItem label="Excluded databases">
                            {task.excludedDatabases.join(", ")}
                        </RichPanelDetailItem>
                    )}
                    {task.taskType === "Backup" ? (
                        <>
                            <RichPanelDetailItem label="Backup type">{task.backupType}</RichPanelDetailItem>
                            <RichPanelDetailItem label="Destinations">
                                {task.backupDestinations.length > 0
                                    ? task.backupDestinations.join(", ")
                                    : "No destinations defined"}
                            </RichPanelDetailItem>
                            <RichPanelDetailItem label="Encrypted">
                                {task.isEncrypted ? "Yes" : "No"}
                            </RichPanelDetailItem>
                        </>
                    ) : (
                        <>
                            <RichPanelDetailItem label="Topology discovery URLs">
                                {task.topologyDiscoveryUrls.join(", ")}
                            </RichPanelDetailItem>
                            {task.delayReplicationFor && task.delayReplicationFor !== "00:00:00" && (
                                <RichPanelDetailItem label="Replication delay">
                                    {task.delayReplicationFor}
                                </RichPanelDetailItem>
                            )}
                        </>
                    )}
                </RichPanelDetails>
            )}
        </RichPanel>
    );
}
