import "../../tasks/ongoingTasks/OngoingTaskPage.scss";
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
import {
    OngoingTaskInfo,
    OngoingTaskSharedInfo,
    OngoingTaskEmbeddingsGenerationInfo,
    OngoingTaskGenAiInfo,
} from "components/models/tasks";
import { useAppSelector } from "components/store";
import TaskUtils from "components/utils/TaskUtils";
import etlScriptDefinitionCache from "models/database/stats/etlScriptDefinitionCache";
import { useReducer, useState, useCallback, useEffect } from "react";
import Button from "react-bootstrap/Button";
import Row from "react-bootstrap/Row";
import { EmbeddingsGenerationPanel } from "../../tasks/ongoingTasks/panels/EmbeddingsGenerationPanel";
import OngoingTaskOperationConfirm from "../../tasks/shared/OngoingTaskOperationConfirm";
import { useOngoingTasksOperations, BaseOngoingTaskPanelProps, taskKey } from "../../tasks/shared/shared";
import { Icon } from "components/common/Icon";
import AiTasksInfoHub from "./AiTasksInfoHub";
import OngoingTaskAddModal from "../../tasks/ongoingTasks/partials/OngoingTaskAddModal";
import OngoingTaskSelectActions from "../../tasks/ongoingTasks/partials/OngoingTaskSelectActions";
import {
    ongoingTasksReducer,
    ongoingTasksReducerInitializer,
} from "../../tasks/ongoingTasks/partials/OngoingTasksReducer";
import { EtlProgressProvider } from "../../tasks/ongoingTasks/partials/OngoingTaskProgressProviders";
import { GenAiPanel } from "../../tasks/ongoingTasks/panels/GenAiPanel";

type EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;

export default function AiTasks() {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const { tasksService } = useServices();
    const [tasks, dispatch] = useReducer(ongoingTasksReducer, db, ongoingTasksReducerInitializer);

    const { value: isNewTaskModalOpen, toggle: toggleIsNewTaskModalOpen } = useBoolean(false);
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
    }, [db]);

    const onEtlProgress = useCallback(
        (progress: EtlTaskProgress[], location: databaseLocationSpecifier) => {
            dispatch({
                type: "EtlProgressLoaded",
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

    const embeddingsGenerations = tasks.tasks.filter(
        (x) => x.shared.taskType === "EmbeddingsGeneration"
    ) as OngoingTaskEmbeddingsGenerationInfo[];

    const genAiTasks = tasks.tasks.filter((x) => x.shared.taskType === "GenAi") as OngoingTaskGenAiInfo[];

    const getSelectedTaskShardedInfos = () =>
        [...embeddingsGenerations, ...genAiTasks]
            .filter((x) => selectedTaskIds.includes(x.shared.taskId))
            .map((x) => x.shared);

    const filteredDatabaseTaskIds = Object.values([...embeddingsGenerations, ...genAiTasks])
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

    return (
        <div className="content-padding ongoing-tasks-page">
            {progressEnabled && <EtlProgressProvider onProgress={onEtlProgress} />}
            {operationConfirm && <OngoingTaskOperationConfirm {...operationConfirm} toggle={cancelOperationConfirm} />}
            <StickyHeader>
                <div className="hstack gap-3 flex-wrap">
                    {hasDatabaseWriteAccess && (
                        <>
                            {isNewTaskModalOpen && (
                                <OngoingTaskAddModal
                                    toggle={toggleIsNewTaskModalOpen}
                                    subscriptionsDatabaseCount={0}
                                    isAiOnly
                                />
                            )}
                            <div id="NewTaskButton">
                                <Button onClick={toggleIsNewTaskModalOpen} variant="primary" className="rounded-pill">
                                    <Icon icon="ongoing-tasks" addon="plus" />
                                    Add AI Task
                                </Button>
                            </div>
                        </>
                    )}
                    <FlexGrow />
                    <AiTasksInfoHub />
                </div>
                {(embeddingsGenerations.length > 0 || genAiTasks.length > 0) && hasDatabaseAdminAccess && (
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
                        {embeddingsGenerations.length === 0 && genAiTasks.length === 0 && (
                            <EmptySet>No tasks have been created for this Database Group.</EmptySet>
                        )}
                        {genAiTasks.length > 0 && (
                            <div key="genAI">
                                <HrHeader className="ai-etl" count={genAiTasks.length}>
                                    <Icon icon="genai" />
                                    GenAI
                                </HrHeader>

                                {genAiTasks.map((x) => (
                                    <GenAiPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        )}
                        {embeddingsGenerations.length > 0 && (
                            <div key="embeddings-generations">
                                <HrHeader className="ai-etl" count={embeddingsGenerations.length}>
                                    <Icon icon="ai-etl" />
                                    Embeddings Generation
                                </HrHeader>

                                {embeddingsGenerations.map((x) => (
                                    <EmbeddingsGenerationPanel
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
