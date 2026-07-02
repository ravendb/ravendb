import { TaskWithErrors, TasksFiltersState, getTaskHealthStatus, resolveStudioTaskType } from "./tasksErrorsUtils";
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;

export function filterTasksWithErrors(
    tasksWithErrors: TaskWithErrors[],
    etlStats: EtlTaskStats[],
    filters: TasksFiltersState
): TaskWithErrors[] {
    const { searchText, nodeTags, shardNumbers, healthStatuses, taskTypes } = filters;

    return tasksWithErrors
        .filter((task) => {
            const studioTaskType = resolveStudioTaskType(task.category, task.etlType);
            const matchesTaskType = !taskTypes.length || (studioTaskType != null && taskTypes.includes(studioTaskType));
            const taskHealth = getTaskHealthStatus(etlStats, task.etlName);
            const matchesHealth = !healthStatuses.length || healthStatuses.includes(taskHealth);
            return matchesTaskType && matchesHealth;
        })
        .map((task) => ({
            ...task,
            transformations: task.transformations
                .filter((t) => {
                    const matchesSearch =
                        !searchText ||
                        task.etlName.toLowerCase().includes(searchText.toLowerCase()) ||
                        t.transformationName.toLowerCase().includes(searchText.toLowerCase());
                    return matchesSearch;
                })
                .map((t) => ({
                    ...t,
                    itemErrors: t.itemErrors.filter(
                        (e) =>
                            (!nodeTags.length || nodeTags.includes(e.nodeTag)) &&
                            (!shardNumbers.length || shardNumbers.includes(String(e.shardNumber)))
                    ),
                    processErrors: t.processErrors.filter(
                        (e) =>
                            (!nodeTags.length || nodeTags.includes(e.nodeTag)) &&
                            (!shardNumbers.length || shardNumbers.includes(String(e.shardNumber)))
                    ),
                }))
                .filter((t) => t.itemErrors.length > 0 || t.processErrors.length > 0),
        }))
        .filter((task) => task.transformations.length > 0);
}
