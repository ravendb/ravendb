import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { StickyHeader } from "components/common/StickyHeader";
import { useRavenLink } from "hooks/useRavenLink";
import OngoingTaskSelectActions from "./OngoingTaskSelectActions";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { OngoingTasksState } from "components/pages/database/tasks/ongoingTasks/partials/OngoingTasksReducer";
import appUrl from "common/appUrl";
import OngoingTasksFilter, {
    OngoingTaskFilterType,
    OngoingTasksFilterCriteria,
} from "components/pages/database/tasks/ongoingTasks/partials/OngoingTasksFilter";
import { InputItem } from "components/models/common";
import { exhaustiveStringTuple } from "components/utils/common";
import assertUnreachable from "components/utils/assertUnreachable";
import { useOngoingTasksOperations } from "components/pages/database/tasks/shared/shared";
import OngoingTaskOperationConfirm from "components/pages/database/tasks/shared/OngoingTaskOperationConfirm";
import { useAppUrls } from "hooks/useAppUrls";
import AiTasksInfoHub from "components/pages/database/aiHub/aiTasks/AiTasksInfoHub";

interface OngoingTasksHeaderProps {
    tasks: OngoingTasksState;
    hasInternalReplication: boolean;
    allTasksCount: number;
    selectedTaskIds: number[];
    filter: OngoingTasksFilterCriteria;
    setFilter: (x: OngoingTasksFilterCriteria) => void;
    reload: () => void;
    filteredDatabaseTaskIds: number[];
    setSelectedTaskIds: (tasks: number[]) => void;
    isAiOnly?: boolean;
}

export function OngoingTasksHeader(props: OngoingTasksHeaderProps) {
    const { forCurrentDatabase } = useAppUrls();
    const {
        tasks,
        allTasksCount,
        selectedTaskIds,
        setFilter,
        filter,
        hasInternalReplication,
        filteredDatabaseTaskIds,
        reload,
        setSelectedTaskIds,
        isAiOnly,
    } = props;

    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const serverWideTasksUrl = appUrl.forServerWideTasks();

    const { onTaskOperation, isTogglingStateAny, isDeletingAny, operationConfirm, cancelOperationConfirm } =
        useOngoingTasksOperations(reload);

    const getSelectedTaskShardedInfos = () =>
        [...tasks.tasks, ...tasks.subscriptions, ...tasks.replicationHubs]
            .filter((x) => selectedTaskIds.includes(x.shared.taskId))
            .map((x) => x.shared);

    return (
        <>
            <StickyHeader>
                <div className="hstack gap-3 flex-wrap">
                    {hasDatabaseWriteAccess && (
                        <>
                            <div id="NewTaskButton">
                                <Button
                                    href={forCurrentDatabase.addNewOngoingTask(isAiOnly)()}
                                    className="rounded-pill"
                                >
                                    <Icon icon="ongoing-tasks" addon="plus" />
                                    {isAiOnly ? "Add AI Task" : "Add a Database Task"}
                                </Button>
                            </div>
                        </>
                    )}

                    <FlexGrow />

                    {isClusterAdminOrClusterNode && !isAiOnly && (
                        <Button
                            variant="link"
                            size="sm"
                            target="_blank"
                            href={serverWideTasksUrl}
                            title="Go to the Server-Wide Tasks view"
                        >
                            <Icon icon="server-wide-tasks" />
                            Server-Wide Tasks
                        </Button>
                    )}

                    {isAiOnly ? <AiTasksInfoHub /> : <AboutView />}
                </div>

                {allTasksCount > 0 && (
                    <div className="mt-3">
                        <OngoingTasksFilter
                            filter={filter}
                            setFilter={setFilter}
                            filterByStatusOptions={getFilterByStatusOptions(tasks, hasInternalReplication)}
                            tasksCount={allTasksCount}
                            isAiOnly={isAiOnly}
                        />
                    </div>
                )}
                {allTasksCount > 0 && hasDatabaseAdminAccess && (
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
            {operationConfirm && <OngoingTaskOperationConfirm {...operationConfirm} toggle={cancelOperationConfirm} />}
        </>
    );
}

function AboutView() {
    const ongoingTasksDocsLink = useRavenLink({ hash: "K4ZTNA" });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
                targetId="about-view"
            >
                <div>
                    <ul>
                        <li>
                            <strong>Ongoing-tasks are work tasks assigned to the database</strong>.
                            <br /> A few examples are: Executing a periodic backup of the database, replicating to
                            another RavenDB instance, or transferring data to external frameworks such as Kafka,
                            RabbitMQ, Azure Queue Storage etc.
                        </li>
                        <li className="mt-1">
                            <strong>This view lists all ongoing tasks defined for the database.</strong>
                            <br /> Click the &quot;Add a Database Task&quot; button to view all available tasks and
                            select from the list.
                        </li>
                        <li className="mt-1">
                            <strong>Running in the background</strong>,<br /> each ongoing task is handled by a
                            designated node from the Database-Group nodes:
                            <ul className="margin-top-xxs">
                                <li>
                                    For each task, you can specify which node will be responsible for the task and
                                    whether the cluster may assign a different node when that node is down.
                                </li>
                                <li className="margin-top-xxs">
                                    If not specified, the cluster will decide which node will handle the task.
                                </li>
                            </ul>
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={ongoingTasksDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Ongoing Tasks
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}

function getFilterByStatusOptions(
    state: OngoingTasksState,
    hasInternalReplication: boolean
): InputItem<OngoingTaskFilterType>[] {
    const backupCount = state.tasks.filter((x) => x.shared.taskType === "Backup").length;
    const subscriptionCount = state.subscriptions.length;

    const etlCount = state.tasks.filter((x) => x.shared.taskType.endsWith("Etl")).length;

    const sinkCount = state.tasks.filter(
        (x) => x.shared.taskType === "KafkaQueueSink" || x.shared.taskType === "RabbitQueueSink"
    ).length;

    const aiCount = state.tasks.filter((x) => ["GenAi", "EmbeddingsGeneration"].includes(x.shared.taskType)).length;
    const internalReplicationCount = hasInternalReplication ? 1 : 0;
    const replicationHubCount = state.replicationHubs.length;
    const replicationSinkCount = state.tasks.filter((x) => x.shared.taskType === "PullReplicationAsSink").length;
    const externalReplicationCount = state.tasks.filter((x) => x.shared.taskType === "Replication").length;
    const replicationCount =
        externalReplicationCount + replicationHubCount + replicationSinkCount + internalReplicationCount;

    return exhaustiveStringTuple<OngoingTaskFilterType>()(
        "AI",
        "Replication",
        "ETL",
        "Sink",
        "Backup",
        "Subscription"
    ).map((filterType) => {
        switch (filterType) {
            case "AI":
                return {
                    label: filterType,
                    value: filterType,
                    count: aiCount,
                };
            case "Replication":
                return {
                    label: filterType,
                    value: filterType,
                    count: replicationCount,
                };
            case "ETL":
                return { label: filterType, value: filterType, count: etlCount };
            case "Sink":
                return { label: filterType, value: filterType, count: sinkCount };
            case "Backup":
                return { label: filterType, value: filterType, count: backupCount };
            case "Subscription":
                return { label: filterType, value: filterType, count: subscriptionCount };
            default:
                assertUnreachable(filterType);
        }
    });
}
