import React, { ReactNode } from "react";
import { OngoingTaskSharedInfo } from "components/models/tasks";
import assertUnreachable from "components/utils/assertUnreachable";
import { capitalize } from "lodash";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import { TextColor } from "components/models/common";
import RichAlert from "components/common/RichAlert";
import Button from "react-bootstrap/Button";
import OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;
import Modal from "components/common/Modal";
import classNames from "classnames";

export type OngoingTaskOperationConfirmType = "enable" | "disable" | "delete";

type DestinationStatus = Exclude<OngoingTaskState, "None" | "PartiallyEnabled"> | "Removed";

interface TaskGroup {
    title: string | ReactNode;
    tasks: OngoingTaskSharedInfo[];
    destinationStatus?: DestinationStatus;
}

interface AffectedTasksGrouped {
    disabling?: OngoingTaskSharedInfo[];
    enabling?: OngoingTaskSharedInfo[];
    skipping?: OngoingTaskSharedInfo[];
}

interface OngoingTaskOperationConfirmProps {
    type: OngoingTaskOperationConfirmType;
    taskSharedInfos: OngoingTaskSharedInfo[];
    toggle: () => void;
    onConfirm: () => void;
}

export default function OngoingTaskOperationConfirm(props: OngoingTaskOperationConfirmProps) {
    const { type, taskSharedInfos, toggle, onConfirm } = props;

    const taskGroups = getTaskGroups(type, taskSharedInfos).filter((x) => x.tasks.length > 0);
    const warningMessage = getWarningMessage(taskGroups);

    const onSubmit = () => {
        onConfirm();
        toggle();
    };

    return (
        <Modal show onHide={toggle} contentClassName={`modal-border bulge-${getTypeColor(type)}`}>
            {taskGroups.map((taskGroup, idx) => (
                <div key={"task-group-" + idx}>
                    <Modal.Header className="vstack gap-4" onCloseClick={toggle}>
                        <Icon
                            icon="ongoing-tasks"
                            color={getTypeColor(type)}
                            addon={getTypeIcon(type)}
                            className="fs-1"
                            margin="m-0"
                        />
                        <div className="text-center lead">{taskGroup.title}</div>
                    </Modal.Header>
                    <Modal.Body className="">
                        {taskGroup.tasks.map((task) => (
                            <div key={task.taskId} className="d-flex">
                                <div
                                    className={classNames(
                                        "bg-faded-primary rounded-pill px-2 py-1 d-flex me-2 align-self-start"
                                    )}
                                >
                                    <Icon
                                        icon={getStatusIcon(task.taskState)}
                                        color={getStatusColor(task.taskState)}
                                        margin="m-0"
                                    />
                                    {taskGroup.destinationStatus && (
                                        <>
                                            <Icon
                                                icon="arrow-thin-right"
                                                margin="mx-1"
                                                className="fs-6 align-self-center"
                                            />
                                            <Icon
                                                icon={getStatusIcon(taskGroup.destinationStatus)}
                                                color={getStatusColor(taskGroup.destinationStatus)}
                                                margin="m-0"
                                            />
                                        </>
                                    )}
                                </div>
                                <div className="word-break align-self-center">{task.taskName}</div>
                            </div>
                        ))}
                    </Modal.Body>
                    {idx < taskGroups.length - 1 && <hr className="m-0" />}
                </div>
            ))}
            {warningMessage && <RichAlert variant="warning">{warningMessage}</RichAlert>}
            <Modal.Footer>
                <Button variant="link" onClick={toggle} className="link-muted">
                    Cancel
                </Button>
                <Button variant={getTypeColor(type)} onClick={onSubmit} className="rounded-pill">
                    <Icon icon={getTypeIcon(type)} />
                    {getInfinitiveForType(type)}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

function getInfinitiveForType(type: OngoingTaskOperationConfirmType) {
    return capitalize(type);
}

function getTypeColor(type: OngoingTaskOperationConfirmType): TextColor {
    switch (type) {
        case "enable":
            return "success";
        case "disable":
        case "delete":
            return "danger";
        default:
            assertUnreachable(type);
    }
}

function getTypeIcon(type: OngoingTaskOperationConfirmType): IconName {
    switch (type) {
        case "enable":
            return "start";
        case "disable":
            return "stop";
        case "delete":
            return "trash";
        default:
            assertUnreachable(type);
    }
}

function getStatusColor(status: OngoingTaskState | DestinationStatus): TextColor {
    switch (status) {
        case "Enabled":
            return "success";
        case "Disabled":
        case "Removed":
            return "danger";
        default:
            return "primary";
    }
}

function getStatusIcon(status: OngoingTaskState | DestinationStatus): IconName {
    switch (status) {
        case "Enabled":
            return "start";
        case "Disabled":
            return "stop";
        case "Removed":
            return "trash";
        default:
            return "ongoing-tasks";
    }
}

function getTaskGroups(type: OngoingTaskOperationConfirmType, tasks: OngoingTaskSharedInfo[]): TaskGroup[] {
    switch (type) {
        case "enable": {
            const affectedTaskGrouped = tasks.reduce(
                (accumulator: AffectedTasksGrouped, currentValue: OngoingTaskSharedInfo) => {
                    if (currentValue.taskState === "Enabled" || currentValue.taskState === "PartiallyEnabled") {
                        accumulator.skipping.push({ ...currentValue, taskState: "Enabled" });
                    } else {
                        accumulator.enabling.push(currentValue);
                    }

                    return accumulator;
                },
                {
                    enabling: [],
                    skipping: [],
                }
            );

            return [
                {
                    title: (
                        <>
                            You&apos;re about to <strong className="text-success">enable</strong> following tasks
                        </>
                    ),
                    tasks: affectedTaskGrouped.enabling,
                    destinationStatus: "Enabled",
                },
                {
                    title: "Skipping already enabled tasks",
                    tasks: affectedTaskGrouped.skipping,
                },
            ];
        }
        case "disable": {
            const affectedTaskGrouped = tasks.reduce(
                (accumulator: AffectedTasksGrouped, currentValue: OngoingTaskSharedInfo) => {
                    if (currentValue.taskState === "Disabled") {
                        accumulator.skipping.push(currentValue);
                    } else {
                        accumulator.disabling.push(currentValue);
                    }

                    return accumulator;
                },
                {
                    disabling: [],
                    skipping: [],
                }
            );

            return [
                {
                    title: (
                        <>
                            You&apos;re about to <strong className="text-danger">disable</strong> following tasks
                        </>
                    ),
                    tasks: affectedTaskGrouped.disabling,
                    destinationStatus: "Disabled",
                },
                {
                    title: "Skipping already disabled tasks",
                    tasks: affectedTaskGrouped.skipping,
                },
            ];
        }
        case "delete": {
            return [
                {
                    title: (
                        <>
                            You&apos;re about to <strong className="text-danger">delete</strong> following tasks
                        </>
                    ),
                    tasks,
                    destinationStatus: "Removed",
                },
            ];
        }
        default:
            assertUnreachable(type);
    }
}

function getWarningMessage(taskGroups: TaskGroup[]): ReactNode {
    const allDisableTasks = taskGroups.find((x) => x.destinationStatus === "Disabled")?.tasks ?? [];
    const subscriptionDisableTasksCount = allDisableTasks.filter((x) => x.taskType === "Subscription").length;

    const allDisableTasksCount = allDisableTasks.length;

    if (allDisableTasksCount === 0 || allDisableTasksCount === subscriptionDisableTasksCount) {
        return null;
    }

    if (subscriptionDisableTasksCount === 0) {
        return (
            <small>
                Please note, <strong>disabling</strong>
                {allDisableTasksCount === 1 ? " this task " : " these tasks "}will lead to continuous tombstone
                accumulation until
                {allDisableTasksCount === 1 ? " it is " : " they are "}re-enabled or deleted, resulting in increased
                disk space usage.
            </small>
        );
    }

    return (
        <small>
            Please note, <strong>disabling</strong> the selected tasks (excluding <strong>Subscription</strong> tasks)
            will lead to continuous tombstone accumulation until they are re-enabled or deleted, which will increase
            disk space usage. This warning does not apply to Subscription tasks.
        </small>
    );
}
