import { useState } from "react";
import messagePublisher from "common/messagePublisher";
import { useServices } from "components/hooks/useServices";
import { OngoingTaskSharedInfo } from "components/models/tasks";
import { OngoingTaskOperationConfirmType } from "components/pages/database/tasks/shared/OngoingTaskOperationConfirm";
import assertUnreachable from "components/utils/assertUnreachable";
import { ServerWideTaskSharedInfo } from "./serverWideTaskModels";

interface OperationConfirm {
    type: OngoingTaskOperationConfirmType;
    onConfirm: () => void;
    tasks: ServerWideTaskSharedInfo[];
}

// OngoingTaskOperationConfirm renders only taskId, taskName, taskState and taskType
export function toOperationConfirmInfo(task: ServerWideTaskSharedInfo): OngoingTaskSharedInfo {
    return {
        taskId: task.taskId,
        taskName: task.taskName,
        taskState: task.taskState,
        taskType: task.taskType,
    } as OngoingTaskSharedInfo;
}

export function useServerWideTasksOperations(reload: () => void) {
    const { manageServerService } = useServices();

    const [togglingTaskNames, setTogglingTaskNames] = useState<string[]>([]);
    const [deletingTaskNames, setDeletingTaskNames] = useState<string[]>([]);
    const [operationConfirm, setOperationConfirm] = useState<OperationConfirm>(null);

    const toggleTasks = async (enable: boolean, tasks: ServerWideTaskSharedInfo[]) => {
        const names = tasks.map((x) => x.taskName);
        try {
            setTogglingTaskNames((prev) => [...prev, ...names]);

            const requests = tasks
                .filter((task) => {
                    if ((task.taskState === "Enabled" || task.taskState === "PartiallyEnabled") && enable) {
                        return false;
                    }
                    if (task.taskState === "Disabled" && !enable) {
                        return false;
                    }
                    return true;
                })
                .map((task) => manageServerService.toggleServerWideTask(task.taskType, task.taskName, !enable));

            if (requests.length === 0) {
                return;
            }

            await Promise.all(requests);
            messagePublisher.reportSuccess(
                `${requests.length === 1 ? "Task" : "Tasks"} ${enable ? "enabled" : "disabled"} successfully.`
            );
            reload();
        } finally {
            setTogglingTaskNames((prev) => prev.filter((x) => !names.includes(x)));
        }
    };

    const deleteTasks = async (tasks: ServerWideTaskSharedInfo[]) => {
        const names = tasks.map((x) => x.taskName);
        try {
            setDeletingTaskNames((prev) => [...prev, ...names]);

            await Promise.all(
                tasks.map((task) => manageServerService.deleteServerWideTask(task.taskType, task.taskName))
            );

            messagePublisher.reportSuccess(`${tasks.length === 1 ? "Task" : "Tasks"} deleted successfully.`);
            reload();
        } finally {
            setDeletingTaskNames((prev) => prev.filter((x) => !names.includes(x)));
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
        isDeleting: (name: string) => deletingTaskNames.includes(name),
        isTogglingState: (name: string) => togglingTaskNames.includes(name),
        isDeletingAny: deletingTaskNames.length > 0,
        isTogglingStateAny: togglingTaskNames.length > 0,
    };
}
