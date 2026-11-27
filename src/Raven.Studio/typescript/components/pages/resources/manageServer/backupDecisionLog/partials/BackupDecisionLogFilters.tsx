import { Dispatch, SetStateAction } from "react";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import { HrHeader } from "components/common/HrHeader";
import {
    CheckboxListFilterCategory,
    FilterCategory,
    FilterChips,
    FilterMenu,
} from "components/common/filterMenu/FilterMenu";
import useDebouncedInput from "components/hooks/useDebouncedInput";
import { BackupDecisionCategory, BackupDecisionFilters, BackupDecisionLogTask } from "../backupDecisionLogTypes";
import { backupDecisionCategories, getEventLabel } from "../backupDecisionLogUtils";

interface BackupDecisionLogFiltersProps {
    filters: BackupDecisionFilters;
    onChange: Dispatch<SetStateAction<BackupDecisionFilters>>;
    tasks: BackupDecisionLogTask[];
    databaseNames: string[];
    matchCount: number | null;
}

export default function BackupDecisionLogFilters({
    filters,
    onChange,
    tasks,
    databaseNames,
    matchCount,
}: BackupDecisionLogFiltersProps) {
    const { localValue: reason, handleChange: handleReasonChange } = useDebouncedInput({
        value: filters.reason,
        onDebouncedUpdate: (reason: string) => onChange((prev) => ({ ...prev, reason })),
    });

    const categories: FilterCategory[] = [
        {
            key: "reason",
            label: "Reason",
            icon: "search",
            type: "text",
            value: reason,
            placeholder: "e.g. Task disabled",
            onChange: handleReasonChange,
        },
        {
            key: "event",
            label: "Event",
            icon: "stats",
            type: "checkboxList",
            options: backupDecisionCategories.map((x) => ({ value: x, label: getEventLabel(x) })),
            selectedValues: filters.events,
            onChange: (events) => onChange((prev) => ({ ...prev, events })),
        } satisfies CheckboxListFilterCategory<BackupDecisionCategory>,
        {
            key: "database",
            label: "Database",
            icon: "database",
            type: "checkboxList",
            options: databaseNames.map((x) => ({ value: x, label: x })),
            selectedValues: filters.databases,
            onChange: (databases) => onChange((prev) => ({ ...prev, databases })),
        },
        {
            key: "task",
            label: "Task",
            icon: "tasks",
            type: "checkboxList",
            options: tasks.map((x) => ({ value: String(x.TaskId), label: `${x.TaskName} (${x.Database})` })),
            selectedValues: filters.taskIds,
            onChange: (taskIds) => onChange((prev) => ({ ...prev, taskIds })),
        },
        {
            key: "date",
            label: "Date range",
            icon: "calendar",
            type: "dateRange",
            value: { start: filters.dateFrom, end: filters.dateTo },
            onChange: ({ start, end }) => onChange((prev) => ({ ...prev, dateFrom: start, dateTo: end })),
        },
    ];

    return (
        <>
            <HrHeader
                count={matchCount}
                right={
                    <ButtonGroup size="sm">
                        <FilterMenu categories={categories} title="Filter decisions" label="Filter" />
                    </ButtonGroup>
                }
            >
                Decisions
            </HrHeader>
            <FilterChips categories={categories} className="mb-3" />
        </>
    );
}
