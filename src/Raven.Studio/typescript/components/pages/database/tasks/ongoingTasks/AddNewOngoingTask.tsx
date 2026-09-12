import "./AddNewOngoingTask.scss";
import { AboutViewHeading } from "components/common/AboutView";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import React from "react";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import classNames from "classnames";
import { useAppUrls } from "hooks/useAppUrls";
import { useNewOngoingTasks } from "components/pages/database/tasks/shared/shared";
import { Checkbox } from "components/common/Checkbox";
import { RadioToggleWithIcon } from "components/common/toggles/RadioToggle";
import { AddNewOngoingTaskAboutView } from "components/pages/database/tasks/ongoingTasks/partials/AddNewOngoingTaskAboutView";
import { AddTaskCardList, TaskCardCategory } from "components/pages/database/tasks/shared/AddTaskCardList";
import { useTaskCardDisplayMode } from "components/pages/database/tasks/shared/useTaskCardDisplayMode";

interface AddNewOngoingTaskQueryParams {
    isAiOnly: boolean;
}

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
    const ongoingTasksUrl = forCurrentDatabase.ongoingTasksUrl();
    const aiTasksUrl = forCurrentDatabase.aiTasks();

    const { displayMode, setDisplayMode } = useTaskCardDisplayMode();

    return (
        <div className="content-margin add-new-ongoing-task d-flex flex-column">
            <div className="d-flex justify-content-between align-items-start">
                <AboutViewHeading
                    title={isAiOnly ? "Add AI task" : "Add a database task"}
                    icon="tasks"
                    iconAddon="plus"
                    backUrl={isAiOnly ? aiTasksUrl : ongoingTasksUrl}
                    marginBottom={4}
                />
                <div className="d-flex align-items-center gap-3">
                    <RadioToggleWithIcon
                        name="task-display-mode"
                        leftItem={{ label: "", value: "expanded", iconName: "list" }}
                        rightItem={{ label: "", value: "compact", iconName: "grid-3x2" }}
                        selectedValue={displayMode}
                        setSelectedValue={(val) => setDisplayMode(val)}
                    />
                    <AddNewOngoingTaskAboutView />
                </div>
            </div>
            <div className="add-new-ongoing-task-layout d-flex gap-4 mt-2">
                <div className="add-new-ongoing-task-sidebar flex-shrink-0 p-3">
                    <TaskSearchInput searchText={searchText} setSearchText={setSearchText} className="mb-3" />
                    <TaskCategoryFilter
                        categories={allCategories}
                        availableCategories={searchFilteredTasks}
                        selectedCategories={selectedCategories}
                        onToggle={toggleCategory}
                        onReset={resetCategories}
                    />
                    <hr className="my-3" />
                    <div className="small ms-1 text-muted">Need a cluster-wide task? Check out:</div>
                    <a href={serverWideTasksUrl} className="add-new-ongoing-task-nav-item text-decoration-none">
                        <Icon icon="server-wide-tasks" margin="m-0" />
                        <span>Server-Wide Tasks</span>
                    </a>
                </div>
                <div className="add-new-ongoing-task-content pb-4">
                    <AddTaskCardList categories={filteredTasks} isAiOnly={isAiOnly} displayMode={displayMode} />
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
    categories,
    availableCategories,
    selectedCategories,
    onToggle,
    onReset,
}: {
    categories: CategoryNavItem[];
    availableCategories: TaskCardCategory[];
    selectedCategories: string[];
    onToggle: (categoryName: string) => void;
    onReset: () => void;
}) {
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
        </div>
    );
}
