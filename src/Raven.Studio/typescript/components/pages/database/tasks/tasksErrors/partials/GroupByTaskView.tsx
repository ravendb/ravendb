import React, { useMemo } from "react";
import { EmptySet } from "components/common/EmptySet";
import { EtlTaskWithErrors, TasksFiltersState } from "../utils/tasksErrorsUtils";
import { filterTasksWithErrors } from "../utils/filterTasksErrors";
import { TaskPanel } from "./TaskPanel";
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;

interface GroupByTaskViewProps {
    tasksWithErrors: EtlTaskWithErrors[];
    etlStats: EtlTaskStats[];
    filters: TasksFiltersState;
    onRefresh: () => void;
}

export function GroupByTaskView({ tasksWithErrors, etlStats, filters, onRefresh }: GroupByTaskViewProps) {
    const filteredTasksWithErrors = useMemo(
        () => filterTasksWithErrors(tasksWithErrors, etlStats, filters),
        [tasksWithErrors, etlStats, filters]
    );

    if (filteredTasksWithErrors.length === 0) {
        return <EmptySet>No tasks match the current filters.</EmptySet>;
    }

    return filteredTasksWithErrors.map((task, index) => (
        <TaskPanel {...task} etlStats={etlStats} onRefresh={onRefresh} key={task.etlName + index} />
    ));
}
