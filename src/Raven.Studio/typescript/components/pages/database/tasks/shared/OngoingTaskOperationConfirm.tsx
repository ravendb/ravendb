import React, { ReactNode } from "react";
import { OngoingTaskSharedInfo } from "components/models/tasks";
import assertUnreachable from "components/utils/assertUnreachable";
import OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;
import { capitalize } from "lodash";
import { Icon } from "components/common/Icon";
import classNames = require("classnames");
import { Modal, ModalBody, Button, ModalFooter } from "reactstrap";
import IconName from "typings/server/icons";

export type OngoingTaskOperationConfirmType = "enable" | "disable" | "delete";

type DestinationStatus = Exclude<OngoingTaskState, "None" | "PartiallyEnabled"> | "Removed";

interface TaskGroup {
    title: string | ReactNode;
    tasks: AffectedTasksInfo[];
    destinationStatus?: DestinationStatus;
}

interface AffectedTasksInfo {
    name: string;
    currentStatus: OngoingTaskState;
}

interface AffectedTasksGrouped {
    disabling?: AffectedTasksInfo[];
    enabling?: AffectedTasksInfo[];
    skipping?: AffectedTasksInfo[];
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

    const onSubmit = () => {
        onConfirm();
        toggle();
    };

    return (
        <Modal
            isOpen
            toggle={toggle}
            wrapClassName="bs5"
            contentClassName={`modal-border bulge-${getTypeColor(type)}`}
            centered
        >
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon
                        icon="ongoing-tasks"
                        color={getTypeColor(type)}
                        addon={getTypeIcon(type)}
                        className="fs-1"
                        margin="m-0"
                    />
                </div>
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggle} />
                </div>
                {taskGroups.map((taskGroup, idx) => (
                    <div key={"task-group-" + idx}>
                        <div className="text-center lead">{taskGroup.title}</div>
                        <div className="vstack gap-1 my-4">
                            {taskGroup.tasks.map((task) => (
                                <div key={task.name} className="d-flex">
                                    <div
                                        className={classNames(
                                            "bg-faded-primary rounded-pill px-2 py-1 d-flex me-2 align-self-start"
                                        )}
                                    >
                                        <Icon
                                            icon={getStatusIcon(task.currentStatus)}
                                            color={getStatusColor(task.currentStatus)}
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
                                    <div className="word-break align-self-center">{task.name}</div>
                                </div>
                            ))}
                        </div>
                        {idx < taskGroups.length - 1 && <hr className="m-0" />}
                    </div>
                ))}
            </ModalBody>
            <ModalFooter>
                <Button color="link" onClick={toggle} className="text-muted">
                    Cancel
                </Button>
                <Button color={getTypeColor(type)} onClick={onSubmit} className="rounded-pill">
                    <Icon icon={getTypeIcon(type)} />
                    {getInfinitiveForType(type)}
                </Button>
            </ModalFooter>
        </Modal>
    );
}

function getInfinitiveForType(type: OngoingTaskOperationConfirmType) {
    return capitalize(type);
}

function getTypeColor(type: OngoingTaskOperationConfirmType): string {
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

function getStatusColor(status: OngoingTaskState | DestinationStatus): string {
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
                        accumulator.skipping.push({ name: currentValue.taskName, currentStatus: "Enabled" });
                    } else {
                        accumulator.enabling.push({
                            name: currentValue.taskName,
                            currentStatus: currentValue.taskState,
                        });
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
                        accumulator.skipping.push({ name: currentValue.taskName, currentStatus: "Disabled" });
                    } else {
                        accumulator.disabling.push({
                            name: currentValue.taskName,
                            currentStatus: currentValue.taskState,
                        });
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
                    tasks: tasks.map((x) => ({
                        currentStatus: x.taskState,
                        name: x.taskName,
                    })),
                    destinationStatus: "Removed",
                },
            ];
        }
        default:
            assertUnreachable(type);
    }
}
