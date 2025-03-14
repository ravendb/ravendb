import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { StickyHeader } from "components/common/StickyHeader";
import React from "react";
import OngoingTaskAddModal from "./OngoingTaskAddModal";
import { useRavenLink } from "hooks/useRavenLink";
import OngoingTaskSelectActions from "./OngoingTaskSelectActions";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import useBoolean from "hooks/useBoolean";
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

interface OngoingTasksHeaderProps {
    tasks: OngoingTasksState;
    hasInternalReplication: boolean;
    allTasksCount: number;
    selectedTaskIds: number[];
    subscriptionsDatabaseCount: number;
    filter: OngoingTasksFilterCriteria;
    setFilter: (x: OngoingTasksFilterCriteria) => void;
    reload: () => void;
    filteredDatabaseTaskIds: number[];
    setSelectedTaskIds: (tasks: number[]) => void;
}
export function OngoingTasksHeader(props: OngoingTasksHeaderProps) {
    const {
        tasks,
        allTasksCount,
        selectedTaskIds,
        subscriptionsDatabaseCount,
        setFilter,
        filter,
        hasInternalReplication,
        filteredDatabaseTaskIds,
        reload,
        setSelectedTaskIds,
    } = props;
    const ongoingTasksDocsLink = useRavenLink({ hash: "K4ZTNA" });

    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const { value: isNewTaskModalOpen, toggle: toggleIsNewTaskModalOpen } = useBoolean(false);

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
                            {isNewTaskModalOpen && (
                                <OngoingTaskAddModal
                                    toggle={toggleIsNewTaskModalOpen}
                                    subscriptionsDatabaseCount={subscriptionsDatabaseCount}
                                />
                            )}
                            <div id="NewTaskButton">
                                <Button onClick={toggleIsNewTaskModalOpen} variant="primary" className="rounded-pill">
                                    <Icon icon="ongoing-tasks" addon="plus" />
                                    Add a Database Task
                                </Button>
                            </div>
                        </>
                    )}

                    <FlexGrow />

                    {isClusterAdminOrClusterNode && (
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

                    <AboutViewFloating>
                        <AccordionItemWrapper
                            icon="about"
                            color="info"
                            heading="About this view"
                            description="Get additional info on this feature"
                            targetId="about-view"
                        >
                            <div>
                                <strong>Ongoing-tasks</strong> are work tasks assigned to the database.
                                <ul className="margin-top-xxs">
                                    <li>
                                        A few examples are: <br />
                                        Executing a periodic backup of the database, replicating to another RavenDB
                                        instance, or transferring data to external frameworks such as Kafka, RabbitMQ,
                                        Azure Queue Storage etc.
                                    </li>
                                    <li className="margin-top-xxs">
                                        Click the &quot;Add a Database Task&quot; button to view all available tasks and
                                        select from the list.
                                    </li>
                                </ul>
                            </div>
                            <div>
                                <strong>Running in the background</strong>, each ongoing task is handled by a designated
                                node from the Database-Group nodes.
                                <ul className="margin-top-xxs">
                                    <li>
                                        For each task, you can specify which node will be responsible for the task and
                                        whether the cluster may assign a different node when that node is down.
                                    </li>
                                    <li className="margin-top-xxs">
                                        If not specified, the cluster will decide which node will handle the task.
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
                </div>

                {allTasksCount > 0 && (
                    <div className="mt-3">
                        <OngoingTasksFilter
                            filter={filter}
                            setFilter={setFilter}
                            filterByStatusOptions={getFilterByStatusOptions(tasks, hasInternalReplication)}
                            tasksCount={allTasksCount}
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

    const internalReplicationCount = hasInternalReplication ? 1 : 0;
    const replicationHubCount = state.replicationHubs.length;
    const replicationSinkCount = state.tasks.filter((x) => x.shared.taskType === "PullReplicationAsSink").length;
    const externalReplicationCount = state.tasks.filter((x) => x.shared.taskType === "Replication").length;
    const replicationCount =
        externalReplicationCount + replicationHubCount + replicationSinkCount + internalReplicationCount;

    return exhaustiveStringTuple<OngoingTaskFilterType>()("Replication", "ETL", "Sink", "Backup", "Subscription").map(
        (filterType) => {
            switch (filterType) {
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
        }
    );
}
