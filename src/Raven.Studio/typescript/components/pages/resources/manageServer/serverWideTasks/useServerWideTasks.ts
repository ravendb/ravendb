import { useMemo, useState } from "react";
import { useAsync } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import { InputItem } from "components/models/common";
import {
    mapServerWideTaskFromDto,
    ServerWideBackupTaskInfo,
    ServerWideExternalReplicationTaskInfo,
    ServerWideTaskInfo,
    ServerWideTaskType,
} from "./serverWideTaskModels";

export function useServerWideTasks() {
    const { manageServerService } = useServices();

    const [nameFilter, setNameFilter] = useState("");
    const [selectedTypes, setSelectedTypes] = useState<ServerWideTaskType[]>([]);

    const asyncGetTasks = useAsync(() => manageServerService.getAllServerWideTasks(), [], {
        // Preserve the previous result (and "success" status) while re-fetching,
        // so the list stays mounted during reload after enable/disable/delete.
        // The default setLoading resets the state to { status: "loading", result: undefined }.
        setLoading: (state) => ({ ...state, loading: true }),
    });

    const tasks: ServerWideTaskInfo[] = useMemo(
        () =>
            (asyncGetTasks.result?.Tasks ?? [])
                .map(mapServerWideTaskFromDto)
                .sort((a, b) => a.taskName.toLocaleLowerCase().localeCompare(b.taskName.toLocaleLowerCase())),
        [asyncGetTasks.result]
    );

    const filteredTasks = useMemo(() => {
        const nameLower = nameFilter.trim().toLowerCase();
        return tasks.filter(
            (task) =>
                (!nameLower || task.taskName.toLowerCase().includes(nameLower)) &&
                (selectedTypes.length === 0 || selectedTypes.includes(task.taskType))
        );
    }, [tasks, nameFilter, selectedTypes]);

    const replicationTasks = filteredTasks.filter(
        (x): x is ServerWideExternalReplicationTaskInfo => x.taskType === "Replication"
    );
    const backupTasks = filteredTasks.filter((x): x is ServerWideBackupTaskInfo => x.taskType === "Backup");

    const typeFilterItems: InputItem<ServerWideTaskType>[] = [
        {
            value: "Replication",
            label: "External replication",
            count: tasks.filter((x) => x.taskType === "Replication").length,
        },
        { value: "Backup", label: "Backup", count: tasks.filter((x) => x.taskType === "Backup").length },
    ];

    return {
        fetchStatus: asyncGetTasks.status,
        reload: asyncGetTasks.execute,
        tasks,
        filteredTasks,
        replicationTasks,
        backupTasks,
        nameFilter,
        setNameFilter,
        selectedTypes,
        setSelectedTypes,
        typeFilterItems,
    };
}
