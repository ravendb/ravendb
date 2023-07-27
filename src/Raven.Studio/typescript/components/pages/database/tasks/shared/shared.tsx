import database from "models/resources/database";
import {
    AnyEtlOngoingTaskInfo,
    OngoingEtlTaskNodeInfo,
    OngoingTaskInfo,
    OngoingTaskSharedInfo,
} from "../../../../models/tasks";
import useBoolean from "hooks/useBoolean";
import React, { useCallback, useState } from "react";
import router from "plugins/router";
import { withPreventDefault } from "../../../../utils/common";
import { RichPanelDetailItem, RichPanelName } from "../../../../common/RichPanel";
import {
    Button,
    ButtonGroup,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    Spinner,
    UncontrolledDropdown,
} from "reactstrap";
import { Icon } from "components/common/Icon";
import { OngoingTaskOperationConfirmType } from "./OngoingTaskOperationConfirm";
import assertUnreachable from "components/utils/assertUnreachable";
import messagePublisher from "common/messagePublisher";
import ModifyOngoingTaskResult = Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult;
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

export interface BaseOngoingTaskPanelProps<T extends OngoingTaskInfo> {
    db: database;
    data: T;
    isSelected: (taskName: string) => boolean;
    toggleSelection: (checked: boolean, taskName: OngoingTaskSharedInfo) => void;
    onToggleDetails?: (newState: boolean) => void;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => void;
    isDeleting: (taskName: string) => boolean;
    isTogglingState: (taskName: string) => boolean;
}

export interface ICanShowTransformationScriptPreview {
    showItemPreview: (task: OngoingTaskInfo, scriptName: string) => void;
}

export function useTasksOperations(editUrl: string, props: BaseOngoingTaskPanelProps<OngoingTaskInfo>) {
    const { onToggleDetails } = props;
    const { value: detailsVisible, toggle: toggleDetailsVisible } = useBoolean(false);

    const onEdit = useCallback(() => {
        router.navigate(editUrl);
    }, [editUrl]);

    const toggleDetails = useCallback(() => {
        toggleDetailsVisible();
        onToggleDetails?.(!detailsVisible);
    }, [onToggleDetails, toggleDetailsVisible, detailsVisible]);

    return {
        detailsVisible,
        toggleDetails,
        onEdit,
    };
}

export function OngoingTaskResponsibleNode(props: { task: OngoingTaskInfo }) {
    const { task } = props;
    const preferredMentor = task.shared.mentorNodeTag;
    const currentNode = task.shared.responsibleNodeTag;

    const usingNotPreferredNode = preferredMentor && currentNode ? preferredMentor !== currentNode : false;

    if (currentNode) {
        return (
            <div className="node">
                {usingNotPreferredNode ? (
                    <>
                        <span className="text-danger pulse" title="User preferred node for this task">
                            <Icon icon="cluster-node" />
                            {preferredMentor}
                        </span>

                        <span className="text-success" title="Cluster node that is temporary responsible for this task">
                            <Icon icon="arrow-right" color="danger" className="pulse" />
                            {currentNode}
                        </span>
                    </>
                ) : (
                    <span
                        title={
                            task.shared.taskType === "PullReplicationAsHub"
                                ? "Hub node that is serving this Sink task"
                                : "Cluster node that is responsible for this task"
                        }
                    >
                        <Icon icon="cluster-node" />
                        {currentNode}
                    </span>
                )}
            </div>
        );
    }

    return (
        <div title="No node is currently handling this task">
            <Icon icon="cluster-node" /> N/A
        </div>
    );
}

export function OngoingTaskName(props: { task: OngoingTaskInfo; canEdit: boolean; editUrl: string }) {
    const { task, editUrl } = props;
    return (
        <RichPanelName>
            <a href={editUrl} title={"Task name: " + task.shared.taskName}>
                {task.shared.taskName}
            </a>
        </RichPanelName>
    );
}

interface OngoingTaskStatusProps {
    task: OngoingTaskInfo;
    canEdit: boolean;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => void;
    isTogglingState: boolean;
}

export function OngoingTaskStatus(props: OngoingTaskStatusProps) {
    const { task, canEdit, onTaskOperation, isTogglingState } = props;
    return (
        <UncontrolledDropdown>
            <DropdownToggle
                caret
                disabled={!canEdit || isTogglingState}
                color={task.shared.taskState === "Disabled" ? "warning" : "secondary"}
            >
                {isTogglingState && <Spinner size="sm" />} {task.shared.taskState}
            </DropdownToggle>
            <DropdownMenu>
                <DropdownItem onClick={() => onTaskOperation("enable", [task.shared])}>
                    <Icon icon="play" color="success" /> Enable
                </DropdownItem>
                <DropdownItem onClick={() => onTaskOperation("disable", [task.shared])}>
                    <Icon icon="stop" color="danger" />
                    Disable
                </DropdownItem>
            </DropdownMenu>
        </UncontrolledDropdown>
    );
}

interface OngoingTaskActionsProps {
    canEdit: boolean;
    task: OngoingTaskInfo;
    toggleDetails: () => void;
    onEdit: () => void;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => void;
    isDeleting: boolean;
}

export function OngoingTaskActions(props: OngoingTaskActionsProps) {
    const { canEdit, task, onEdit, toggleDetails, onTaskOperation, isDeleting } = props;

    return (
        <div className="actions">
            <ButtonGroup>
                <Button onClick={toggleDetails} title="Click for details">
                    <Icon icon="info" margin="m-0" />
                </Button>
                {!task.shared.serverWide && (
                    <Button onClick={onEdit} title="Edit task">
                        <Icon icon="edit" margin="m-0" />
                    </Button>
                )}
                {!task.shared.serverWide && (
                    <ButtonWithSpinner
                        color="danger"
                        disabled={!canEdit}
                        isSpinning={isDeleting}
                        onClick={() => onTaskOperation("delete", [task.shared])}
                        title="Delete task"
                        spinnerMargin="m-0"
                        icon={{
                            icon: "trash",
                            margin: "m-0",
                        }}
                    ></ButtonWithSpinner>
                )}
            </ButtonGroup>
        </div>
    );
}

export function ConnectionStringItem(props: {
    canEdit: boolean;
    connectionStringName: string;
    connectionStringsUrl: string;
    connectionStringDefined: boolean;
}) {
    const { canEdit, connectionStringDefined, connectionStringName, connectionStringsUrl } = props;

    if (connectionStringDefined) {
        return (
            <RichPanelDetailItem label="Connection String">
                {canEdit ? (
                    <a title="Connection string name" target="_blank" href={connectionStringsUrl}>
                        {connectionStringName}
                    </a>
                ) : (
                    <div>{connectionStringName}</div>
                )}
            </RichPanelDetailItem>
        );
    }

    return (
        <RichPanelDetailItem label="Connection String">
            <Icon icon="danger" color="danger" />
            <span className="text-danger">This connection string is not defined.</span>
        </RichPanelDetailItem>
    );
}

export function EmptyScriptsWarning(props: { task: AnyEtlOngoingTaskInfo }) {
    const emptyScripts = findScriptsWithOutMatchingDocuments(props.task);

    if (!emptyScripts.length) {
        return null;
    }

    return (
        <RichPanelDetailItem className="text-warning">
            <small>
                <Icon icon="warning" />
                Following scripts don&apos;t match any documents: {emptyScripts.join(", ")}
            </small>
        </RichPanelDetailItem>
    );
}

function findScriptsWithOutMatchingDocuments(
    data: OngoingTaskInfo<OngoingTaskSharedInfo, OngoingEtlTaskNodeInfo>
): string[] {
    const perScriptCounts = new Map<string, number>();
    data.nodesInfo.forEach((node) => {
        if (node.etlProgress) {
            node.etlProgress.forEach((progress) => {
                const transformationName = progress.transformationName;
                perScriptCounts.set(
                    transformationName,
                    (perScriptCounts.get(transformationName) ?? 0) + progress.global.total
                );
            });
        }
    });

    return Array.from(perScriptCounts.entries())
        .filter((x) => x[1] === 0)
        .map((x) => x[0]);
}

export function taskKey(task: OngoingTaskSharedInfo) {
    // we don't want to use taskId here - as it changes after edit
    return task.taskType + "-" + task.taskName;
}

interface OperationConfirm {
    type: OngoingTaskOperationConfirmType;
    onConfirm: () => void;
    taskSharedInfos: OngoingTaskSharedInfo[];
}

export function useOngoingTasksOperations(database: database, reload: () => void) {
    const { tasksService } = useServices();

    const [togglingTaskNames, setTogglingTaskNames] = useState<string[]>([]);
    const [deletingTaskNames, setDeletingTaskNames] = useState<string[]>([]);

    const [operationConfirm, setOperationConfirm] = useState<OperationConfirm>(null);

    const toggleOngoingTasks = async (enable: boolean, taskSharedInfos: OngoingTaskSharedInfo[]) => {
        try {
            setTogglingTaskNames((names) => [...names, ...taskSharedInfos.map((x) => x.taskName)]);
            const toggleRequests: Promise<ModifyOngoingTaskResult>[] = [];

            for (const task of taskSharedInfos) {
                if ((task.taskState === "Enabled" || task.taskState === "PartiallyEnabled") && enable) {
                    continue;
                }
                if (task.taskState === "Disabled" && !enable) {
                    continue;
                }

                toggleRequests.push(tasksService.toggleOngoingTask(database, task, enable));
            }

            if (toggleRequests.length === 0) {
                return;
            }

            await Promise.all(toggleRequests);
            messagePublisher.reportSuccess(
                `${toggleRequests.length === 1 ? "Task" : "Tasks"} ${enable ? "enabled" : "disabled"} successfully.`
            );
            reload();
        } finally {
            setTogglingTaskNames((names) => names.filter((x) => !taskSharedInfos.map((x) => x.taskName).includes(x)));
        }
    };

    const deleteOngoingTasks = async (taskSharedInfos: OngoingTaskSharedInfo[]) => {
        try {
            setDeletingTaskNames((names) => [...names, ...taskSharedInfos.map((x) => x.taskName)]);

            const deleteRequests: Promise<ModifyOngoingTaskResult>[] = taskSharedInfos.map((task) =>
                tasksService.deleteOngoingTask(database, task)
            );

            await Promise.all(deleteRequests);

            messagePublisher.reportSuccess(`${deleteRequests.length === 1 ? "Task" : "Tasks"} deleted successfully.`);
            reload();
        } finally {
            setDeletingTaskNames((names) => names.filter((x) => !taskSharedInfos.map((x) => x.taskName).includes(x)));
        }
    };

    const onTaskOperation = (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => {
        switch (type) {
            case "enable": {
                setOperationConfirm({
                    type: "enable",
                    onConfirm: () => toggleOngoingTasks(true, taskSharedInfos),
                    taskSharedInfos,
                });
                break;
            }
            case "disable": {
                setOperationConfirm({
                    type: "disable",
                    onConfirm: () => toggleOngoingTasks(false, taskSharedInfos),
                    taskSharedInfos,
                });
                break;
            }
            case "delete": {
                setOperationConfirm({
                    type: "delete",
                    onConfirm: () => deleteOngoingTasks(taskSharedInfos),
                    taskSharedInfos,
                });
                break;
            }
            default:
                assertUnreachable(type);
        }
    };

    return {
        onTaskOperation,
        operationConfirm,
        cancelOperationConfirm: () => setOperationConfirm(null),
        isDeleting: (taskName: string) => deletingTaskNames.includes(taskName),
        isTogglingState: (taskName: string) => togglingTaskNames.includes(taskName),
        isDeletingAny: deletingTaskNames.length > 0,
        isTogglingStateAny: togglingTaskNames.length > 0,
    };
}
