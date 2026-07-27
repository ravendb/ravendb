import "components/pages/database/tasks/ongoingTasks/AddNewOngoingTask.scss";
import React, { useState } from "react";
import appUrl from "common/appUrl";
import { AboutViewHeading } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { AddTaskCardList, TaskCardCategory } from "components/pages/database/tasks/shared/AddTaskCardList";
import { TaskCategoryFilter, TaskSearchInput } from "components/pages/database/tasks/ongoingTasks/AddNewOngoingTask";
import { RadioToggleWithIcon } from "components/common/toggles/RadioToggle";
import { useTaskCardDisplayMode } from "components/pages/database/tasks/shared/useTaskCardDisplayMode";
import { PerDatabaseOngoingTasksLink } from "./partials/PerDatabaseOngoingTasksLink";
import { ServerWideTasksInfoHub } from "./partials/ServerWideTasksInfoHub";

export default function AddServerWideTask() {
    const hasServerWideBackups = useAppSelector(licenseSelectors.statusValue("HasServerWideBackups"));
    const hasServerWideExternalReplications = useAppSelector(
        licenseSelectors.statusValue("HasServerWideExternalReplications")
    );

    const { displayMode, setDisplayMode } = useTaskCardDisplayMode();

    const [searchText, setSearchText] = useState("");
    const [selectedCategories, setSelectedCategories] = useState<string[]>([]);

    const toggleCategory = (categoryName: string) => {
        setSelectedCategories((prev) =>
            prev.includes(categoryName) ? prev.filter((c) => c !== categoryName) : [...prev, categoryName]
        );
    };

    const resetCategories = () => setSelectedCategories([]);

    const serverWideTasks: TaskCardCategory[] = [
        {
            categoryName: "External replication",
            categoryIcon: "external-replication",
            tasks: [
                {
                    title: "Server-wide External Replication",
                    description:
                        "Create live replicas of all databases in your cluster, replicating each to a corresponding RavenDB database with the same name in another cluster.",
                    iconName: "external-replication",
                    variant: "Replication",
                    target: "serverWideExternalReplication",
                    link: appUrl.forEditServerWideExternalReplication(),
                    licenseBadge: "Professional +",
                    showLicenseBadge: !hasServerWideExternalReplications,
                },
            ],
        },
        {
            categoryName: "Backup",
            categoryIcon: "backup",
            tasks: [
                {
                    title: "Server-wide Backup",
                    description:
                        "Create periodic backups or snapshots of all databases in your cluster, managing schedule, retention, and destination from a single task.",
                    iconName: "periodic-backup",
                    variant: "Backups",
                    target: "serverWidePeriodicBackup",
                    link: appUrl.forEditServerWideBackup(),
                    licenseBadge: "Professional +",
                    showLicenseBadge: !hasServerWideBackups,
                },
            ],
        },
    ];

    const searchLower = searchText.trim().toLowerCase();

    const searchFilteredTasks = serverWideTasks
        .map((category) => ({
            ...category,
            tasks: category.tasks.filter(
                (task) =>
                    !searchLower ||
                    task.title.toLowerCase().includes(searchLower) ||
                    task.description.toLowerCase().includes(searchLower)
            ),
        }))
        .filter((category) => category.tasks.length > 0);

    const filteredTasks = searchFilteredTasks.filter(
        (category) => selectedCategories.length === 0 || selectedCategories.includes(category.categoryName)
    );

    const allCategories = serverWideTasks.map((c) => ({
        categoryName: c.categoryName,
        categoryIcon: c.categoryIcon,
    }));

    return (
        <div className="content-margin add-new-ongoing-task d-flex flex-column">
            <div className="d-flex justify-content-between align-items-start">
                <AboutViewHeading
                    title="Add a Server-Wide Task"
                    icon="server-wide-tasks"
                    backUrl={appUrl.forServerWideTasks()}
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
                    <ServerWideTasksInfoHub />
                </div>
            </div>
            <div className="add-new-ongoing-task-layout d-flex gap-4 mt-2">
                <div className="add-new-ongoing-task-sidebar flex-shrink-0 p-3">
                    <TaskSearchInput
                        searchText={searchText}
                        setSearchText={setSearchText}
                        placeholder="e.g. Server-wide Backup"
                        className="mb-3"
                    />
                    <TaskCategoryFilter
                        variant="checkbox"
                        categories={allCategories}
                        availableCategories={searchFilteredTasks}
                        selectedCategories={selectedCategories}
                        onToggle={toggleCategory}
                        onReset={resetCategories}
                    />
                    <hr className="my-3" />
                    <div className="small ms-1 text-muted">Need a per-database task? Check out:</div>
                    <PerDatabaseOngoingTasksLink text="Ongoing Tasks" />
                </div>
                <div className="add-new-ongoing-task-content pb-4">
                    <AddTaskCardList categories={filteredTasks} displayMode={displayMode} />
                </div>
            </div>
        </div>
    );
}
