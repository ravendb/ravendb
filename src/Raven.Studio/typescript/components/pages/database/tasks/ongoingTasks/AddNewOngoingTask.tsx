import { HrHeader } from "components/common/HrHeader";
import "./AddNewOngoingTask.scss";
import { AboutViewHeading } from "components/common/AboutView";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import React, { ReactNode, useEffect, useState } from "react";
import studioSettings = require("common/settings/studioSettings");
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import classNames from "classnames";
import { useAppUrls } from "hooks/useAppUrls";
import { useEventsCollector } from "hooks/useEventsCollector";
import LicenseRestrictedBadge, { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import { useNewOngoingTasks } from "components/pages/database/tasks/shared/shared";
import { EmptySet } from "components/common/EmptySet";
import { AddNewOngoingTaskAboutView } from "components/pages/database/tasks/ongoingTasks/partials/AddNewOngoingTaskAboutView";
import { RadioToggleWithIcon } from "components/common/toggles/RadioToggle";
import { Checkbox } from "components/common/Checkbox";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { AccessPopover } from "components/common/AccessPopover";

interface AddNewOngoingTaskQueryParams {
    isAiOnly: boolean;
    noBack?: string;
}

type DisplayMode = "expanded" | "compact";

export default function AddNewOngoingTask({ queryParams }: ReactQueryParamsProps<AddNewOngoingTaskQueryParams>) {
    const isAiOnly = queryParams?.isAiOnly;

    const { forCurrentDatabase, appUrl } = useAppUrls();
    const {
        filteredTasks,
        searchFilteredTasks,
        allCategories,
        searchText,
        setSearchText,
        selectedCategories,
        toggleCategory,
        resetCategories,
    } = useNewOngoingTasks({ isAiOnly });

    const serverWideTasksUrl = appUrl.forServerWideTasks();
    const ongoingTasksUrl = forCurrentDatabase.ongoingTasksUrl(true)();
    const aiTasksUrl = forCurrentDatabase.aiTasks();
    const showBackUrl = !queryParams?.noBack;

    const [displayMode, setDisplayModeState] = useState<DisplayMode>("expanded");

    useEffect(() => {
        let disposed = false;

        studioSettings.default.globalSettings().done((settings) => {
            if (!disposed) {
                setDisplayModeState(settings.ongoingTaskDisplayMode.getValue());
            }
        });

        return () => {
            disposed = true;
        };
    }, []);

    const setDisplayMode = (mode: DisplayMode) => {
        setDisplayModeState(mode);
        studioSettings.default.globalSettings().done((settings) => settings.ongoingTaskDisplayMode.setValue(mode));
    };

    return (
        <div className="add-new-ongoing-task d-flex flex-column">
            <div className="d-flex justify-content-between align-items-start">
                {showBackUrl ? (
                    <AboutViewHeading
                        title={isAiOnly ? "Add AI task" : "Add a database task"}
                        icon="tasks"
                        iconAddon="plus"
                        backUrl={isAiOnly ? aiTasksUrl : ongoingTasksUrl}
                        marginBottom={4}
                    />
                ) : (
                    <AboutViewHeading title="Add a database task" icon="tasks" iconAddon="plus" marginBottom={4} />
                )}
                <div className="d-flex align-items-center gap-3">
                    <RadioToggleWithIcon
                        name="task-display-mode"
                        leftItem={{ label: "", value: "expanded", iconName: "list" }}
                        rightItem={{ label: "", value: "compact", iconName: "grid-3x2" }}
                        selectedValue={displayMode}
                        setSelectedValue={(val) => setDisplayMode(val)}
                    />
                    {!isAiOnly && <AddNewOngoingTaskAboutView />}
                </div>
            </div>
            <div className="add-new-ongoing-task-horizontal-nav gap-1">
                <TaskSearchInput searchText={searchText} setSearchText={setSearchText} className="mb-2" />
                <TaskCategoryFilter
                    variant="chips"
                    categories={allCategories}
                    availableCategories={searchFilteredTasks}
                    selectedCategories={selectedCategories}
                    onToggle={toggleCategory}
                    onReset={resetCategories}
                />
            </div>
            <div className="add-new-ongoing-task-layout d-flex gap-4 mt-2">
                <div className="add-new-ongoing-task-sidebar flex-shrink-0 p-3">
                    <TaskSearchInput searchText={searchText} setSearchText={setSearchText} className="mb-3" />
                    <TaskCategoryFilter
                        variant="checkbox"
                        categories={allCategories}
                        availableCategories={searchFilteredTasks}
                        selectedCategories={selectedCategories}
                        onToggle={toggleCategory}
                        onReset={resetCategories}
                    />
                    <hr className="my-3" />
                    <div className="small ms-1 text-muted">Need a cluster-wide task? Check out:</div>
                    <a
                        href={serverWideTasksUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="add-new-ongoing-task-nav-item text-decoration-none"
                    >
                        <Icon icon="server-wide-tasks" margin="m-0" />
                        <span>Server-Wide Tasks</span>
                        <Icon icon="newtab" margin="ms-0 m-0" className="add-new-ongoing-task-nav-item__newtab" />
                    </a>
                </div>
                <div className="add-new-ongoing-task-content pb-4">
                    <OngoingTasksList filteredTasks={filteredTasks} isAiOnly={isAiOnly} displayMode={displayMode} />
                </div>
            </div>
        </div>
    );
}

function TaskSearchInput({
    searchText,
    setSearchText,
    className,
}: {
    searchText: string;
    setSearchText: (value: string) => void;
    className?: string;
}) {
    return (
        <div className={className}>
            <div className="small-label ms-1 mb-1">Search by name</div>
            <Form.Control
                type="search"
                accessKey="/"
                placeholder="e.g. External Replication"
                title="Filter tasks"
                className="filtering-input"
                value={searchText}
                onChange={(e) => setSearchText(e.target.value)}
            />
        </div>
    );
}

interface CategoryNavItem {
    categoryName: string;
    categoryIcon: IconName;
}

function TaskCategoryFilter({
    variant,
    categories,
    availableCategories,
    selectedCategories,
    onToggle,
    onReset,
}: {
    variant: "chips" | "checkbox";
    categories: CategoryNavItem[];
    availableCategories: TaskCategory[];
    selectedCategories: string[];
    onToggle: (categoryName: string) => void;
    onReset: () => void;
}) {
    const isChips = variant === "chips";
    const hasActiveFilter = selectedCategories.length > 0;

    return (
        <div>
            <div className="d-flex justify-content-between align-items-center mb-1">
                <div className="small-label">Filter by Category</div>
                <Button
                    variant="link"
                    size="xs"
                    className={classNames("p-0", { invisible: !hasActiveFilter })}
                    onClick={onReset}
                    disabled={!hasActiveFilter}
                >
                    Reset
                    <Icon icon="reset" margin="ms-1" />
                </Button>
            </div>
            {isChips ? (
                <div className="add-new-ongoing-task-chips-row">
                    {categories.map((category) => {
                        const isAvailable = availableCategories.some((c) => c.categoryName === category.categoryName);
                        return (
                            <button
                                key={category.categoryName}
                                className={classNames("add-new-ongoing-task-chip", {
                                    active: selectedCategories.includes(category.categoryName),
                                })}
                                onClick={() => onToggle(category.categoryName)}
                                disabled={!isAvailable}
                            >
                                <Icon icon={category.categoryIcon} margin="m-0" />
                                <span>{category.categoryName}</span>
                            </button>
                        );
                    })}
                </div>
            ) : (
                <div className="d-flex flex-column">
                    {categories.map((category) => {
                        const isAvailable = availableCategories.some((c) => c.categoryName === category.categoryName);
                        return (
                            <Checkbox
                                key={category.categoryName}
                                selected={selectedCategories.includes(category.categoryName)}
                                toggleSelection={() => onToggle(category.categoryName)}
                                disabled={!isAvailable}
                                className="add-new-ongoing-task-filter-item"
                            >
                                {category.categoryName}
                            </Checkbox>
                        );
                    })}
                </div>
            )}
        </div>
    );
}

interface TaskCategory {
    categoryName: string;
    categoryHeaderName?: string;
    categoryIcon: IconName;
    tasks: TaskItemProps[];
}

interface OngoingTasksListProps {
    filteredTasks: TaskCategory[];
    isAiOnly: boolean;
    displayMode?: DisplayMode;
}

export function OngoingTasksList({ filteredTasks, isAiOnly, displayMode = "expanded" }: OngoingTasksListProps) {
    if (filteredTasks.length === 0) {
        return <EmptySet>No tasks match your filter criteria</EmptySet>;
    }

    const isCompact = displayMode === "compact";

    return (
        <>
            {filteredTasks.map((category) => (
                <div className="pb-2" key={category.categoryName}>
                    {!isAiOnly && (
                        <HrHeader>
                            <Icon icon={category.categoryIcon} />
                            {category.categoryHeaderName ?? category.categoryName}
                        </HrHeader>
                    )}
                    <div
                        className={classNames(
                            "d-grid ongoing-tasks-grid",
                            isCompact ? "gap-2 ongoing-tasks-grid--compact" : "gap-3"
                        )}
                    >
                        {category.tasks.map((task) => (
                            <TaskItem key={task.title} {...task} displayMode={displayMode} />
                        ))}
                    </div>
                </div>
            ))}
        </>
    );
}

type TaskCardVariant = "AI" | "Replication" | "Backups" | "Subscriptions" | "ETL" | "Sink";

export interface TaskItemProps {
    title: string;
    description: string;
    iconName: IconName;
    variant: TaskCardVariant;
    link: string;
    target: string;
    licenseBadge?: LicenseBadgeText;
    counterBadge?: ReactNode;
    showLicenseBadge?: boolean;
    isShardingSupported?: boolean;
    accessRequired: databaseAccessLevel;
    customDisabledReason?: ReactNode;
    displayMode?: DisplayMode;
}

function TaskItem({
    title,
    description,
    link,
    iconName,
    target,
    variant,
    licenseBadge,
    showLicenseBadge,
    counterBadge,
    isShardingSupported,
    accessRequired,
    customDisabledReason,
    displayMode = "expanded",
}: TaskItemProps) {
    const { reportEvent } = useEventsCollector();
    const isSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded;
    const canHandleOperation = useAppSelector(accessManagerSelectors.getCanHandleOperation)(accessRequired);

    const isShardingNotSupported = !isShardingSupported && isSharded;
    const isDisabled = isShardingNotSupported || !canHandleOperation || !!customDisabledReason;

    const isCompact = displayMode === "compact";

    return (
        <AccessPopover
            className="w-100 h-100"
            accessRequired={accessRequired}
            conditions={[
                {
                    isActive: isShardingNotSupported,
                    message: "Sharding is not supported for this task",
                },
                {
                    isActive: !!customDisabledReason,
                    message: customDisabledReason,
                },
            ]}
        >
            <a
                href={isDisabled ? undefined : link}
                onClick={() => reportEvent(target, "new")}
                className={classNames(
                    "card no-decor w-100 ongoing-tasks-card h-100 add-new-ongoing-task-card",
                    `variant-${variant}`,
                    {
                        "item-disabled": !!isDisabled,
                        compact: isCompact,
                    }
                )}
            >
                <Card.Body className={isCompact ? "d-flex align-items-center" : "d-flex flex-column gap-1"}>
                    <div className="d-flex align-items-center">
                        <Icon icon={iconName} className="task-icon" margin="me-2" />
                        <h4 className="mb-0">{title}</h4>
                        {counterBadge}
                    </div>
                    {!isCompact && <div className="small">{description}</div>}
                </Card.Body>

                {showLicenseBadge && licenseBadge && (
                    <LicenseRestrictedBadge
                        className="position-absolute top-0 end-0 m-2"
                        licenseRequired={licenseBadge}
                    />
                )}
            </a>
        </AccessPopover>
    );
}
