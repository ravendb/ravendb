import { useState } from "react";
import { pick } from "lodash";
import messagePublisher from "common/messagePublisher";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import { useServices } from "components/hooks/useServices";
import {
    OngoingTaskOperationConfirmType,
    OperationConfirmTaskInfo,
} from "components/pages/database/tasks/shared/OngoingTaskOperationConfirm";
import assertUnreachable from "components/utils/assertUnreachable";
import { ServerWideTaskSharedInfo } from "./serverWideTaskModels";

interface OperationConfirm {
    type: OngoingTaskOperationConfirmType;
    onConfirm: () => void;
    tasks: ServerWideTaskSharedInfo[];
}

export function toOperationConfirmInfo(task: ServerWideTaskSharedInfo): OperationConfirmTaskInfo {
    return pick(task, ["taskId", "taskName", "taskState", "taskType"]);
}

export function useServerWideTasksOperations(reload: () => void) {
    const { manageServerService } = useServices();

    const [togglingTaskIds, setTogglingTaskIds] = useState<number[]>([]);
    const [deletingTaskIds, setDeletingTaskIds] = useState<number[]>([]);
    const [operationConfirm, setOperationConfirm] = useState<OperationConfirm>(null);

    const toggleTasks = async (enable: boolean, tasks: ServerWideTaskSharedInfo[]) => {
        const tasksToToggle = tasks.filter((task) => {
            if ((task.taskState === "Enabled" || task.taskState === "PartiallyEnabled") && enable) {
                return false;
            }
            if (task.taskState === "Disabled" && !enable) {
                return false;
            }
            return true;
        });

        if (tasksToToggle.length === 0) {
            return;
        }

        const ids = tasksToToggle.map((x) => x.taskId);
        try {
            setTogglingTaskIds((prev) => [...prev, ...ids]);

            await Promise.all(
                tasksToToggle.map((task) =>
                    manageServerService.toggleServerWideTask(task.taskType, task.taskName, !enable)
                )
            );
            messagePublisher.reportSuccess(
                `${pluralizeHelpers.pluralize(tasksToToggle.length, "Task", "Tasks", true)} ${
                    enable ? "enabled" : "disabled"
                } successfully.`
            );
            reload();
        } finally {
            setTogglingTaskIds((prev) => prev.filter((x) => !ids.includes(x)));
        }
    };

    const deleteTasks = async (tasks: ServerWideTaskSharedInfo[]) => {
        const ids = tasks.map((x) => x.taskId);
        try {
            setDeletingTaskIds((prev) => [...prev, ...ids]);

            await Promise.all(
                tasks.map((task) => manageServerService.deleteServerWideTask(task.taskType, task.taskName))
            );

            messagePublisher.reportSuccess(
                `${pluralizeHelpers.pluralize(tasks.length, "Task", "Tasks", true)} deleted successfully.`
            );
            reload();
        } finally {
            setDeletingTaskIds((prev) => prev.filter((x) => !ids.includes(x)));
        }
    };

    const onTaskOperation = (type: OngoingTaskOperationConfirmType, tasks: ServerWideTaskSharedInfo[]) => {
        switch (type) {
            case "enable":
                setOperationConfirm({ type, onConfirm: () => toggleTasks(true, tasks), tasks });
                break;
            case "disable":
                setOperationConfirm({ type, onConfirm: () => toggleTasks(false, tasks), tasks });
                break;
            case "delete":
                setOperationConfirm({ type, onConfirm: () => deleteTasks(tasks), tasks });
                break;
            default:
                assertUnreachable(type);
        }
    };

    return {
        onTaskOperation,
        operationConfirm,
        cancelOperationConfirm: () => setOperationConfirm(null),
        isDeleting: (taskId: number) => deletingTaskIds.includes(taskId),
        isTogglingState: (taskId: number) => togglingTaskIds.includes(taskId),
        isDeletingAny: deletingTaskIds.length > 0,
        isTogglingStateAny: togglingTaskIds.length > 0,
    };
}
