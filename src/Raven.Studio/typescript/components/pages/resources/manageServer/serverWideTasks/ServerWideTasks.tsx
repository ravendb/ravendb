import React, { ReactNode, useEffect, useState } from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import appUrl from "common/appUrl";
import { compareSets } from "common/typeUtils";
import IconName from "typings/server/icons";
import { AboutViewHeading } from "components/common/AboutView";
import { EmptySet } from "components/common/EmptySet";
import { HrHeader } from "components/common/HrHeader";
import { Icon } from "components/common/Icon";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import { MultiCheckboxToggle } from "components/common/toggles/MultiCheckboxToggle";
import OngoingTaskOperationConfirm from "components/pages/database/tasks/shared/OngoingTaskOperationConfirm";
import OngoingTaskSelectActions from "components/pages/database/tasks/ongoingTasks/partials/OngoingTaskSelectActions";
import ServerWideTaskPanel from "./partials/ServerWideTaskPanel";
import { ServerWideTasksInfoHub } from "./partials/ServerWideTasksInfoHub";
import { useServerWideTasks } from "./useServerWideTasks";
import { useServerWideTasksOperations, toOperationConfirmInfo } from "./useServerWideTasksOperations";
import { ServerWideTaskInfo, ServerWideTaskSharedInfo } from "./serverWideTaskModels";

export default function ServerWideTasks() {
    const {
        fetchStatus,
        reload,
        tasks,
        filteredTasks,
        replicationTasks,
        backupTasks,
        nameFilter,
        setNameFilter,
        selectedTypes,
        setSelectedTypes,
        typeFilterItems,
    } = useServerWideTasks();

    const {
        onTaskOperation,
        operationConfirm,
        cancelOperationConfirm,
        isDeleting,
        isTogglingState,
        isDeletingAny,
        isTogglingStateAny,
    } = useServerWideTasksOperations(reload);

    const [selectedTaskIds, setSelectedTaskIds] = useState<number[]>([]);

    const filteredTaskIds = filteredTasks.map((x) => x.taskId);

    useEffect(() => {
        const updatedSelectedTaskIds = selectedTaskIds.filter((id) => filteredTaskIds.includes(id));

        if (!compareSets(updatedSelectedTaskIds, selectedTaskIds)) {
            setSelectedTaskIds(updatedSelectedTaskIds);
        }
    }, [filteredTaskIds, selectedTaskIds]);

    const isSelected = (taskId: number) => selectedTaskIds.includes(taskId);

    const toggleSelection = (checked: boolean, task: ServerWideTaskSharedInfo) => {
        if (checked) {
            setSelectedTaskIds((selectedIds) => [...selectedIds, task.taskId]);
        } else {
            setSelectedTaskIds((selectedIds) => selectedIds.filter((x) => x !== task.taskId));
        }
    };

    const getSelectedTasks = () => filteredTasks.filter((x) => selectedTaskIds.includes(x.taskId));

    const panelProps = {
        isSelected,
        toggleSelection,
        onTaskOperation,
        isDeleting,
        isTogglingState,
    };

    return (
        <div className="content-margin">
            <div className="d-flex justify-content-between">
                <AboutViewHeading title="Server-Wide Tasks" icon="server-wide-tasks" />
                <ServerWideTasksInfoHub />
            </div>

            {operationConfirm && (
                <OngoingTaskOperationConfirm
                    type={operationConfirm.type}
                    taskSharedInfos={operationConfirm.tasks.map(toOperationConfirmInfo)}
                    toggle={cancelOperationConfirm}
                    onConfirm={operationConfirm.onConfirm}
                />
            )}

            {fetchStatus === "loading" && <LoadingView />}

            {fetchStatus === "error" && <LoadError error="Unable to load server-wide tasks" refresh={reload} />}

            {fetchStatus === "success" && tasks.length === 0 && (
                <div className="text-center mt-5">
                    <EmptySet>No server-wide tasks configured yet</EmptySet>
                    <div className="text-muted mb-3">
                        Automate backups and replication across your entire cluster
                    </div>
                    <Button variant="primary" className="rounded-pill" href={appUrl.forAddServerWideTask()}>
                        <Icon icon="plus" />
                        Create Server-Wide Task
                    </Button>
                </div>
            )}

            {fetchStatus === "success" && tasks.length > 0 && (
                <>
                    <Button variant="primary" className="rounded-pill mb-3" href={appUrl.forAddServerWideTask()}>
                        <Icon icon="plus" />
                        Add a Server-Wide Task
                    </Button>

                    <div className="d-flex flex-wrap flex-grow align-items-end gap-3 mb-3">
                        <div className="flex-grow">
                            <div className="small-label ms-1 mb-1">Filter by name</div>
                            <div className="clearable-input">
                                <Form.Control
                                    type="text"
                                    accessKey="/"
                                    placeholder="e.g. BackupTask"
                                    title="Filter server-wide tasks"
                                    className="filtering-input"
                                    value={nameFilter}
                                    onChange={(e) => setNameFilter(e.target.value)}
                                />
                                {nameFilter && (
                                    <div className="clear-button">
                                        <Button variant="secondary" size="sm" onClick={() => setNameFilter("")}>
                                            <Icon icon="clear" margin="m-0" />
                                        </Button>
                                    </div>
                                )}
                            </div>
                        </div>
                        <div>
                            <MultiCheckboxToggle
                                inputItems={typeFilterItems}
                                label="Filter by type"
                                selectedItems={selectedTypes}
                                setSelectedItems={setSelectedTypes}
                            />
                        </div>
                    </div>

                    <OngoingTaskSelectActions
                        allTasks={filteredTaskIds}
                        selectedTasks={selectedTaskIds}
                        setSelectedTasks={setSelectedTaskIds}
                        onTaskOperation={(type) => onTaskOperation(type, getSelectedTasks())}
                        isTogglingState={isTogglingStateAny}
                        isDeleting={isDeletingAny}
                    />

                    {filteredTasks.length === 0 && <EmptySet>No tasks match your filter criteria</EmptySet>}

                    <TaskSection title="External replication" icon="external-replication" tasks={replicationTasks}>
                        {(task) => <ServerWideTaskPanel key={taskKey(task)} task={task} {...panelProps} />}
                    </TaskSection>

                    <TaskSection title="Backup" icon="backup" tasks={backupTasks}>
                        {(task) => <ServerWideTaskPanel key={taskKey(task)} task={task} {...panelProps} />}
                    </TaskSection>
                </>
            )}
        </div>
    );
}

function taskKey(task: ServerWideTaskInfo) {
    return task.taskType + "-" + task.taskName;
}

interface TaskSectionProps {
    title: string;
    icon: IconName;
    tasks: ServerWideTaskInfo[];
    children: (task: ServerWideTaskInfo) => ReactNode;
}

function TaskSection(props: TaskSectionProps) {
    const { title, icon, tasks, children } = props;

    if (tasks.length === 0) {
        return null;
    }

    return (
        <div className="mb-4">
            <HrHeader count={tasks.length}>
                <Icon icon={icon} />
                {title}
            </HrHeader>
            <div className="vstack gap-2">{tasks.map(children)}</div>
        </div>
    );
}
