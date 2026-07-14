import React from "react";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import Spinner from "react-bootstrap/Spinner";
import { Checkbox } from "components/common/Checkbox";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { RichPanelDetailItem, RichPanelName, RichPanelSelect } from "components/common/RichPanel";
import { OngoingTaskOperationConfirmType } from "components/pages/database/tasks/shared/OngoingTaskOperationConfirm";
import { ServerWideTaskSharedInfo } from "../serverWideTaskModels";

export interface BaseServerWideTaskPanelProps<T extends ServerWideTaskSharedInfo> {
    task: T;
    isSelected: (taskId: number) => boolean;
    toggleSelection: (checked: boolean, task: ServerWideTaskSharedInfo) => void;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, tasks: ServerWideTaskSharedInfo[]) => void;
    isDeleting: (taskName: string) => boolean;
    isTogglingState: (taskName: string) => boolean;
}

export function ServerWideTaskSelect(props: {
    task: ServerWideTaskSharedInfo;
    isSelected: (taskId: number) => boolean;
    toggleSelection: (checked: boolean, task: ServerWideTaskSharedInfo) => void;
}) {
    const { task, isSelected, toggleSelection } = props;

    return (
        <RichPanelSelect>
            <Checkbox
                selected={isSelected(task.taskId)}
                toggleSelection={(e) => toggleSelection(e.currentTarget.checked, task)}
            />
        </RichPanelSelect>
    );
}

export function ServerWideTaskName(props: { task: ServerWideTaskSharedInfo; editUrl: string }) {
    const { task, editUrl } = props;

    return (
        <RichPanelName>
            <a href={editUrl} title={"Task name: " + task.taskName}>
                {task.taskName}
            </a>
        </RichPanelName>
    );
}

export function ServerWideTaskResponsibleNode(props: { task: ServerWideTaskSharedInfo }) {
    const { task } = props;

    if (!task.responsibleNodeTag) {
        return null;
    }

    return (
        <div title="Cluster node that is responsible for this task">
            <Icon icon="cluster-node" />
            {task.responsibleNodeTag}
        </div>
    );
}

export function ServerWideTaskStatus(props: {
    task: ServerWideTaskSharedInfo;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, tasks: ServerWideTaskSharedInfo[]) => void;
    isTogglingState: boolean;
}) {
    const { task, onTaskOperation, isTogglingState } = props;

    return (
        <Dropdown>
            <Dropdown.Toggle
                disabled={isTogglingState}
                title="Set task state"
                variant={task.taskState === "Disabled" ? "warning" : "secondary"}
            >
                {isTogglingState && <Spinner size="sm" />} {task.taskState}
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
    );
}

export function ServerWideTaskActions(props: {
    task: ServerWideTaskSharedInfo;
    editUrl: string;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, tasks: ServerWideTaskSharedInfo[]) => void;
    isDeleting: boolean;
    detailsVisible: boolean;
    toggleDetails: () => void;
}) {
    const { task, editUrl, onTaskOperation, isDeleting, detailsVisible, toggleDetails } = props;

    return (
        <ButtonGroup>
            <Button variant="secondary" onClick={toggleDetails} title="Click for details">
                <Icon icon={detailsVisible ? "fold" : "unfold"} margin="m-0" />
            </Button>
            <Button variant="secondary" href={editUrl} title="Edit task">
                <Icon icon="edit" margin="m-0" />
            </Button>
            <ButtonWithSpinner
                variant="danger"
                isSpinning={isDeleting}
                onClick={() => onTaskOperation("delete", [task])}
                title="Delete task"
                spinnerMargin="m-0"
                icon={{ icon: "trash", margin: "m-0" }}
            />
        </ButtonGroup>
    );
}

export function ServerWideTaskExcludedDatabases(props: { task: ServerWideTaskSharedInfo }) {
    const { task } = props;

    if (task.excludedDatabases.length === 0) {
        return null;
    }

    return <RichPanelDetailItem label="Excluded databases">{task.excludedDatabases.join(", ")}</RichPanelDetailItem>;
}
