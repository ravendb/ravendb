import { useRef, useState } from "react";
import moment from "moment";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import Form from "react-bootstrap/Form";
import DatePicker from "components/common/DatePicker";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { HrHeader } from "components/common/HrHeader";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import useBoolean from "components/hooks/useBoolean";
import useDebouncedInput from "components/hooks/useDebouncedInput";
import useUniqueId from "components/hooks/useUniqueId";
import {
    BackupDecisionCategory,
    BackupDecisionFilters,
    BackupDecisionLogTask,
    emptyBackupDecisionFilters,
} from "../backupDecisionLogTypes";
import { backupDecisionCategories, getEventLabel } from "../backupDecisionLogUtils";

type FilterCategory = "reason" | "event" | "database" | "task" | "date";

const categoryLabels: Record<FilterCategory, string> = {
    reason: "Reason",
    event: "Event",
    database: "Database",
    task: "Task",
    date: "Date range",
};

const categoryIcons: Record<FilterCategory, IconName> = {
    reason: "search",
    event: "stats",
    database: "database",
    task: "tasks",
    date: "calendar",
};

const chipLabels: Record<FilterCategory, string> = { ...categoryLabels, date: "Date" };

const categories = Object.keys(categoryLabels) as FilterCategory[];

const formatDate = (date: Date | null) => (date ? moment(date).format("DD/MM/YYYY HH:mm") : "...");

function getChipValue(
    category: FilterCategory,
    filters: BackupDecisionFilters,
    tasks: BackupDecisionLogTask[]
): string | null {
    switch (category) {
        case "reason":
            return filters.reason || null;
        case "event":
            return filters.events.length > 0 ? filters.events.map((x) => getEventLabel(x)).join(", ") : null;
        case "database":
            return filters.databases.length > 0 ? filters.databases.join(", ") : null;
        case "task":
            return filters.taskIds.length > 0 ? filters.taskIds.map((x) => getTaskLabel(x, tasks)).join(", ") : null;
        case "date":
            return filters.dateFrom || filters.dateTo
                ? `${formatDate(filters.dateFrom)} - ${formatDate(filters.dateTo)}`
                : null;
    }
}

function getTaskLabel(taskId: string, tasks: BackupDecisionLogTask[]) {
    const task = tasks.find((x) => String(x.TaskId) === taskId);
    return task ? `${task.TaskName} (${task.Database})` : taskId;
}

function toEndOfDayIfMidnight(date: Date | null) {
    return date && moment(date).isSame(moment(date).startOf("day")) ? moment(date).endOf("day").toDate() : date;
}

function clearCategory(category: FilterCategory, filters: BackupDecisionFilters): BackupDecisionFilters {
    switch (category) {
        case "reason":
            return { ...filters, reason: "" };
        case "event":
            return { ...filters, events: [] };
        case "database":
            return { ...filters, databases: [] };
        case "task":
            return { ...filters, taskIds: [] };
        case "date":
            return { ...filters, dateFrom: null, dateTo: null };
    }
}

interface BackupDecisionLogFiltersProps {
    filters: BackupDecisionFilters;
    onChange: (filters: BackupDecisionFilters) => void;
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
    const [category, setCategory] = useState<FilterCategory | null>(null);
    const chipsRef = useRef<HTMLDivElement>(null);
    const { value: isOpen, setValue: setIsOpen, setTrue: open } = useBoolean(false);

    const openCategory = (target: FilterCategory) => {
        setCategory(target);
        open();
    };

    const activeChips = categories
        .map((x) => ({ category: x, value: getChipValue(x, filters, tasks) }))
        .filter((x) => x.value !== null);

    const clearOpenCategory = () => {
        onChange(clearCategory(category, filters));
        setIsOpen(false);
        setCategory(null);
    };

    return (
        <>
            <HrHeader
                count={matchCount}
                right={
                    <Dropdown
                        show={isOpen}
                        autoClose="outside"
                        align="end"
                        onToggle={(nextShow, meta) => {
                            const isChipClick = chipsRef.current?.contains(meta.originalEvent?.target as Node);
                            if (meta.source !== "select" && !isChipClick) {
                                setIsOpen(nextShow);
                                setCategory(null);
                            }
                        }}
                    >
                        <Dropdown.Toggle
                            as={CustomDropdownToggle}
                            variant="secondary"
                            size="sm"
                            className="rounded-pill"
                        >
                            <Icon icon="filter" />
                            Filter
                        </Dropdown.Toggle>
                        <Dropdown.Menu className="p-2" style={{ minWidth: 280 }}>
                            {category === null ? (
                                <>
                                    {categories.map((x) => (
                                        <Dropdown.Item as="button" key={x} onClick={() => setCategory(x)}>
                                            {categoryLabels[x]}
                                        </Dropdown.Item>
                                    ))}
                                    <Dropdown.Divider />
                                    <Dropdown.Item as="button" onClick={() => onChange(emptyBackupDecisionFilters)}>
                                        Clear
                                    </Dropdown.Item>
                                </>
                            ) : (
                                <>
                                    <div className="hstack justify-content-between mb-2">
                                        <Button
                                            variant="link"
                                            size="sm"
                                            className="p-0"
                                            aria-label="Back"
                                            onClick={() => setCategory(null)}
                                        >
                                            <Icon icon="arrow-left" />
                                            {categoryLabels[category]}
                                        </Button>
                                        {getChipValue(category, filters, tasks) !== null && (
                                            <Button
                                                variant="link"
                                                size="sm"
                                                className="p-0"
                                                aria-label={`Clear ${categoryLabels[category]} filter`}
                                                onClick={clearOpenCategory}
                                            >
                                                Clear
                                            </Button>
                                        )}
                                    </div>
                                    <FilterCategoryMenu
                                        category={category}
                                        filters={filters}
                                        onChange={onChange}
                                        tasks={tasks}
                                        databaseNames={databaseNames}
                                    />
                                </>
                            )}
                        </Dropdown.Menu>
                    </Dropdown>
                }
            >
                Decisions
            </HrHeader>
            {activeChips.length > 0 && (
                <div ref={chipsRef} className="hstack gap-2 flex-wrap mb-3">
                    {activeChips.map((chip) => (
                        <Button
                            key={chip.category}
                            variant="link"
                            size="sm"
                            className="hstack gap-2 rounded-pill bg-faded-primary text-decoration-none"
                            style={{
                                border: "1px solid var(--text-faded-primary)",
                                color: "var(--text-faded-primary)",
                            }}
                            aria-label={`${chipLabels[chip.category]}: ${chip.value}`}
                            onClick={() => openCategory(chip.category)}
                        >
                            <span className="hstack">
                                <Icon icon={categoryIcons[chip.category]} />
                                <span className="fw-semibold">{chipLabels[chip.category]}:&nbsp;</span>
                                <span className="text-emphasis">{chip.value}</span>
                            </span>
                            <Icon icon="chevron-down" margin="m-0" />
                        </Button>
                    ))}
                </div>
            )}
        </>
    );
}

interface FilterCategoryMenuProps {
    category: FilterCategory;
    filters: BackupDecisionFilters;
    onChange: (filters: BackupDecisionFilters) => void;
    tasks: BackupDecisionLogTask[];
    databaseNames: string[];
}

function FilterCategoryMenu({ category, filters, onChange, tasks, databaseNames }: FilterCategoryMenuProps) {
    switch (category) {
        case "reason":
            return <ReasonInput filters={filters} onChange={onChange} />;
        case "event":
            return (
                <CheckboxList
                    options={backupDecisionCategories}
                    selected={filters.events}
                    getLabel={getEventLabel}
                    onChange={(events: BackupDecisionCategory[]) => onChange({ ...filters, events })}
                />
            );
        case "database":
            return (
                <CheckboxList
                    options={databaseNames}
                    selected={filters.databases}
                    onChange={(databases) => onChange({ ...filters, databases })}
                />
            );
        case "task":
            return (
                <CheckboxList
                    options={tasks.map((x) => String(x.TaskId))}
                    selected={filters.taskIds}
                    getLabel={(taskId) => getTaskLabel(taskId, tasks)}
                    onChange={(taskIds) => onChange({ ...filters, taskIds })}
                />
            );
        case "date":
            return (
                <div className="vstack gap-2">
                    <Form.Label className="mb-0">From</Form.Label>
                    <DatePicker
                        showTimeSelect
                        selected={filters.dateFrom}
                        onChange={(dateFrom: Date | null) => onChange({ ...filters, dateFrom })}
                    />
                    <Form.Label className="mb-0">To</Form.Label>
                    <DatePicker
                        showTimeSelect
                        selected={filters.dateTo}
                        onChange={(dateTo: Date | null) =>
                            onChange({ ...filters, dateTo: toEndOfDayIfMidnight(dateTo) })
                        }
                    />
                </div>
            );
    }
}

function ReasonInput({ filters, onChange }: Pick<FilterCategoryMenuProps, "filters" | "onChange">) {
    const latestFilters = useRef(filters);
    latestFilters.current = filters;

    const { localValue, handleChange } = useDebouncedInput({
        value: filters.reason,
        onDebouncedUpdate: (reason: string) => onChange({ ...latestFilters.current, reason }),
    });

    return (
        <Form.Control
            autoFocus
            placeholder="e.g. Task disabled"
            value={localValue}
            onChange={(e) => handleChange(e.target.value)}
        />
    );
}

interface CheckboxListProps<T extends string> {
    options: T[];
    selected: T[];
    onChange: (selected: T[]) => void;
    getLabel?: (option: T) => string;
}

function CheckboxList<T extends string>({ options, selected, onChange, getLabel }: CheckboxListProps<T>) {
    const idPrefix = useUniqueId("backup-decision-filter-");

    return (
        <div className="vstack gap-1">
            {options.map((option, index) => (
                <Form.Check
                    key={option}
                    type="checkbox"
                    id={`${idPrefix}-${index}`}
                    label={getLabel?.(option) ?? option}
                    checked={selected.includes(option)}
                    onChange={(e) =>
                        onChange(e.target.checked ? [...selected, option] : selected.filter((x) => x !== option))
                    }
                />
            ))}
        </div>
    );
}
