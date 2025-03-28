import { compareSets } from "common/typeUtils";
import { EmptySet } from "components/common/EmptySet";
import { FlexGrow } from "components/common/FlexGrow";
import { HrHeader } from "components/common/HrHeader";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { StickyHeader } from "components/common/StickyHeader";
import useBoolean from "components/hooks/useBoolean";
import useInterval from "components/hooks/useInterval";
import { useServices } from "components/hooks/useServices";
import { OngoingTaskInfo, OngoingTaskSharedInfo, OngoingTaskAmazonSqsEtlInfo } from "components/models/tasks";
import { useAppSelector } from "components/store";
import TaskUtils from "components/utils/TaskUtils";
import etlScriptDefinitionCache from "models/database/stats/etlScriptDefinitionCache";
import { useReducer, useState, useCallback, useEffect } from "react";
import { Button, Row } from "reactstrap";
import { OngoingTaskProgressProvider } from "../../tasks/ongoingTasks/OngoingTaskProgressProvider";
import OngoingTaskSelectActions from "../../tasks/ongoingTasks/OngoingTaskSelectActions";
import { ongoingTasksReducer, ongoingTasksReducerInitializer } from "../../tasks/ongoingTasks/OngoingTasksReducer";
import { AiEtlPanel } from "../../tasks/ongoingTasks/panels/AiEtlPanel";
import OngoingTaskOperationConfirm from "../../tasks/shared/OngoingTaskOperationConfirm";
import { useOngoingTasksOperations, BaseOngoingTaskPanelProps, taskKey } from "../../tasks/shared/shared";
import { Icon } from "components/common/Icon";
import AiTasksInfoHub from "./AiTasksInfoHub";
import { useAppUrls } from "components/hooks/useAppUrls";
import router from "plugins/router";

type EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;

export default function AiTasks() {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const { appUrl } = useAppUrls();
    const { tasksService } = useServices();
    const [tasks, dispatch] = useReducer(ongoingTasksReducer, db, ongoingTasksReducerInitializer);

    const { value: progressEnabled, setTrue: startTrackingProgress } = useBoolean(false);
    const [definitionCache] = useState(() => new etlScriptDefinitionCache(db.name));

    const fetchTasks = useCallback(
        async (location: databaseLocationSpecifier) => {
            try {
                const tasks = await tasksService.getOngoingTasks(db.name, location);
                dispatch({
                    type: "TasksLoaded",
                    location,
                    tasks,
                });
            } catch (e) {
                dispatch({
                    type: "TasksLoadError",
                    location,
                    error: e,
                });
            }
        },
        [db, tasksService, dispatch]
    );

    const reload = useCallback(async () => {
        // if database is sharded we need to load from both orchestrator and target node point of view
        // in case of non-sharded - we have single level: node

        if (db.isSharded) {
            const orchestratorTasks = db.nodes.map((node) => fetchTasks({ nodeTag: node.tag }));
            await Promise.all(orchestratorTasks);
        }

        const loadTasks = tasks.locations.map(fetchTasks);
        await Promise.all(loadTasks);
    }, [tasks, fetchTasks, db]);

    useInterval(reload, 10_000);

    useEffect(() => {
        reload();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [db]);

    const onEtlProgress = useCallback(
        (progress: EtlTaskProgress[], location: databaseLocationSpecifier) => {
            dispatch({
                type: "ProgressLoaded",
                progress,
                location,
            });
        },
        [dispatch]
    );

    const showItemPreview = useCallback(
        (task: OngoingTaskInfo, scriptName: string) => {
            const taskType = TaskUtils.studioTaskTypeToTaskType(task.shared.taskType);
            const etlType = TaskUtils.taskTypeToEtlType(taskType);
            definitionCache.showDefinitionFor(etlType, task.shared.taskId, scriptName);
        },
        [definitionCache]
    );

    const aiEtls = tasks.tasks.filter((x) => x.shared.taskType === "AiEtl") as OngoingTaskAmazonSqsEtlInfo[];

    const getSelectedTaskShardedInfos = () =>
        [...aiEtls].filter((x) => selectedTaskIds.includes(x.shared.taskId)).map((x) => x.shared);

    const filteredDatabaseTaskIds = Object.values(aiEtls)
        .flat()
        .filter((x) => !x.shared.serverWide)
        .map((x) => x.shared.taskId);

    const [selectedTaskIds, setSelectedTaskIds] = useState<number[]>(filteredDatabaseTaskIds);

    useEffect(() => {
        const updatedSelectedTaskIds = selectedTaskIds.filter((id) => filteredDatabaseTaskIds.includes(id));

        if (!compareSets(updatedSelectedTaskIds, selectedTaskIds)) {
            setSelectedTaskIds(updatedSelectedTaskIds);
        }
    }, [filteredDatabaseTaskIds, selectedTaskIds]);

    const {
        onTaskOperation,
        operationConfirm,
        cancelOperationConfirm,
        isTogglingState,
        isDeleting,
        isTogglingStateAny,
        isDeletingAny,
    } = useOngoingTasksOperations(reload);

    const sharedPanelProps: Omit<BaseOngoingTaskPanelProps<OngoingTaskInfo>, "data"> = {
        onTaskOperation,
        isSelected: (id: number) => selectedTaskIds.includes(id),
        toggleSelection: (checked: boolean, taskShardedInfo: OngoingTaskSharedInfo) => {
            if (checked) {
                setSelectedTaskIds((selectedIds) => [...selectedIds, taskShardedInfo.taskId]);
            } else {
                setSelectedTaskIds((selectedIds) => selectedIds.filter((x) => x !== taskShardedInfo.taskId));
            }
        },
        isTogglingState,
        isDeleting,
    };

    const goToNewAiTask = (e: React.MouseEvent<HTMLButtonElement>) => {
        const url = appUrl.forEditAiEtl(db.name);
        if (e.ctrlKey) {
            window.open(url, "_blank");
        } else {
            router.navigate(url);
        }
    };

    return (
        <div className="content-padding">
            {progressEnabled && <OngoingTaskProgressProvider onEtlProgress={onEtlProgress} />}
            {operationConfirm && <OngoingTaskOperationConfirm {...operationConfirm} toggle={cancelOperationConfirm} />}
            <StickyHeader>
                <div className="hstack gap-3 flex-wrap">
                    {hasDatabaseWriteAccess && (
                        <Button onClick={goToNewAiTask} color="primary" className="rounded-pill">
                            <Icon icon="ongoing-tasks" addon="plus" />
                            Add AI Task
                        </Button>
                    )}
                    <FlexGrow />
                    <AiTasksInfoHub />
                </div>
                {aiEtls.length > 0 && hasDatabaseAdminAccess && (
                    <OngoingTaskSelectActions
                        allTasks={filteredDatabaseTaskIds}
                        selectedTasks={selectedTaskIds}
                        setSelectedTasks={setSelectedTaskIds}
                        onTaskOperation={(type) => onTaskOperation(type, getSelectedTaskShardedInfos())}
                        isTogglingState={isTogglingStateAny}
                        isDeleting={isDeletingAny}
                    />
                )}
            </StickyHeader>
            <Row className="gy-sm">
                <div className="flex-vertical">
                    <div className="scroll flex-grow">
                        {aiEtls.length === 0 && (
                            <EmptySet>No tasks have been created for this Database Group.</EmptySet>
                        )}
                        {aiEtls.length > 0 && (
                            <div key="ai-etls">
                                <HrHeader className="ai-etl" count={aiEtls.length}>
                                    <Icon icon="ai-etl" />
                                    AI ETL
                                </HrHeader>

                                {aiEtls.map((x) => (
                                    <AiEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </Row>
            <div id="modalContainer" className="bs5" />
        </div>
    );
}
