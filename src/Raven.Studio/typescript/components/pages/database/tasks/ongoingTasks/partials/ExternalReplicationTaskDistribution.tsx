import {
    OngoingReplicationProgressAwareTaskNodeInfo,
    OngoingTaskAbstractReplicationNodeInfoDetails,
    OngoingTaskExternalReplicationInfo,
    OngoingTaskInfo,
    OngoingTaskReplicationHubInfo,
    OngoingTaskReplicationSinkInfo,
} from "components/models/tasks";
import {
    ClickableProgress,
    DistributionItem,
    DistributionLegend,
    LocationDistribution,
} from "components/common/LocationDistribution";
import { Icon } from "components/common/Icon";
import { useId, useMemo, useState } from "react";
import classNames from "classnames";
import { ProgressCircle } from "components/common/ProgressCircle";
import { ReplicationProgressDetailsSheet } from "components/pages/database/tasks/ongoingTasks/partials/ReplicationProgressDetailsSheet";
import { databaseLocationComparator, withPreventDefault } from "components/utils/common";
import { ErrorModal } from "components/pages/database/tasks/ongoingTasks/partials/ErrorModal";
import { SheetPortalOutlet, useViewSheet } from "components/common/splitView/ViewSheet";

interface ExternalReplicationTaskDistributionProps {
    task: OngoingTaskExternalReplicationInfo | OngoingTaskReplicationHubInfo | OngoingTaskReplicationSinkInfo;
}

interface TaskDistributionRowProps {
    nodeInfo: OngoingReplicationProgressAwareTaskNodeInfo<OngoingTaskAbstractReplicationNodeInfoDetails>;
    allNodes: OngoingReplicationProgressAwareTaskNodeInfo<OngoingTaskAbstractReplicationNodeInfoDetails>[];
    sharded: boolean;
    task: OngoingTaskExternalReplicationInfo | OngoingTaskReplicationHubInfo | OngoingTaskReplicationSinkInfo;
    isActive: boolean;
    setActiveNodeIndex: (index: number | null) => void;
    ownerId: string;
}

function getTaskTypeLabel(taskType: StudioTaskType): string {
    const labels: Partial<Record<StudioTaskType, string>> = {
        Replication: "External Replication",
        PullReplicationAsHub: "Replication Hub",
        PullReplicationAsSink: "Replication Sink",
    };
    return labels[taskType] ?? taskType;
}

function TaskDistributionRow(props: TaskDistributionRowProps) {
    const { nodeInfo, allNodes, sharded, task, isActive, setActiveNodeIndex, ownerId } = props;

    const shard = (
        <div className="top shard">
            {nodeInfo.location.shardNumber != null && (
                <>
                    <Icon icon="shard" />
                    {nodeInfo.location.shardNumber}
                </>
            )}
        </div>
    );

    const [errorToDisplay, setErrorToDisplay] = useState<string>(null);

    const toggleErrorModal = () => {
        setErrorToDisplay((error) => (error ? null : nodeInfo.details?.error));
    };

    const key = taskNodeInfoKey(task, nodeInfo);
    const hasError = !!nodeInfo.details?.error;

    const { open, renderIntoSheet } = useViewSheet();

    const nodeIndex = allNodes.indexOf(nodeInfo);

    const openProgressSheet = () => {
        setActiveNodeIndex(nodeIndex);
        open({
            ownerId,
            component: <SheetPortalOutlet />,
            initialWidth: "40%",
            minWidth: "25%",
            maxWidth: "60%",
            onClose: () => setActiveNodeIndex(null),
        });
    };

    const canOpenSheet = nodeInfo.status !== "loading" && nodeInfo.status !== "idle";

    return (
        <div>
            <DistributionItem
                loading={nodeInfo.status === "loading" || nodeInfo.status === "idle"}
                key={key}
                className={classNames({ active: isActive })}
            >
                {sharded && shard}
                <div className={classNames("node", { top: !sharded })}>
                    {!sharded && <Icon icon="node" />}

                    {nodeInfo.location.nodeTag}
                </div>
                <div>{nodeInfo.status === "success" ? nodeInfo.details.taskConnectionStatus : ""}</div>
                <div>
                    {nodeInfo.details?.lastDatabaseEtag ? nodeInfo.details.lastDatabaseEtag.toLocaleString() : "-"}
                </div>
                <div>{nodeInfo.details?.lastSentEtag ? nodeInfo.details.lastSentEtag.toLocaleString() : "-"}</div>
                <div>
                    {hasError ? (
                        <a href="#" onClick={withPreventDefault(toggleErrorModal)}>
                            <Icon icon="warning" color="danger" margin="m-0" />
                        </a>
                    ) : (
                        "-"
                    )}
                </div>
                <ExternalReplicationTaskProgress
                    task={task}
                    nodeInfo={nodeInfo}
                    onClick={canOpenSheet ? openProgressSheet : undefined}
                />
            </DistributionItem>
            {errorToDisplay && <ErrorModal key="modal" toggleErrorModal={toggleErrorModal} error={errorToDisplay} />}
            {renderIntoSheet(
                ownerId,
                isActive,
                <ReplicationProgressDetailsSheet
                    key={ownerId}
                    taskType={getTaskTypeLabel(task.shared.taskType)}
                    taskName={task.shared.taskName}
                    allNodes={allNodes}
                    initialNodeIndex={nodeIndex}
                    onNodeChange={setActiveNodeIndex}
                />
            )}
        </div>
    );
}

export function ExternalReplicationTaskDistribution(props: ExternalReplicationTaskDistributionProps) {
    const { task } = props;
    const sharded = task.nodesInfo.some((x) => x.location.shardNumber != null);
    const [activeNodeIndex, setActiveNodeIndex] = useState<number | null>(null);
    const ownerId = useId();
    const { activeSheetOwnerId } = useViewSheet();

    const visibleNodes = useMemo(
        () =>
            task.nodesInfo.filter(
                (nodeInfo) =>
                    nodeInfo.details &&
                    task.responsibleLocations.find((l) => databaseLocationComparator(l, nodeInfo.location))
            ),
        [task]
    );

    const items = visibleNodes.map((nodeInfo, index) => {
        const key = taskNodeInfoKey(task, nodeInfo);

        return (
            <TaskDistributionRow
                key={key}
                nodeInfo={nodeInfo}
                allNodes={visibleNodes}
                sharded={sharded}
                task={task}
                isActive={activeSheetOwnerId === ownerId && activeNodeIndex === index}
                setActiveNodeIndex={setActiveNodeIndex}
                ownerId={ownerId}
            />
        );
    });

    return (
        <div className="px-3 pb-2">
            <LocationDistribution>
                <DistributionLegend>
                    <div className="top"></div>
                    {sharded && (
                        <div className="node">
                            <Icon icon="node" /> Node
                        </div>
                    )}
                    <div>
                        <Icon icon="connected" /> Status
                    </div>
                    <div>
                        <Icon icon="etag" /> Last DB Etag
                    </div>
                    <div>
                        <Icon icon="etag" /> Last Sent Etag
                    </div>
                    <div>
                        <Icon icon="warning" /> Error
                    </div>
                    <div>
                        <Icon icon="changes" /> State
                    </div>
                </DistributionLegend>
                {items}
            </LocationDistribution>
        </div>
    );
}

interface ExternalReplicationTaskProgressProps {
    nodeInfo: OngoingReplicationProgressAwareTaskNodeInfo<OngoingTaskAbstractReplicationNodeInfoDetails>;
    task: OngoingTaskInfo;
    onClick?: () => void;
}

export function ExternalReplicationTaskProgress(props: ExternalReplicationTaskProgressProps) {
    const { nodeInfo, task, onClick } = props;

    const disabled = task.shared.taskState === "Disabled";

    if (!nodeInfo.progress || nodeInfo.progress.length === 0) {
        return (
            <ClickableProgress onClick={onClick}>
                <ProgressCircle icon={disabled ? "stop" : null} state="running" onClick={onClick}>
                    {disabled ? "Disabled" : "?"}
                </ProgressCircle>
            </ClickableProgress>
        );
    }

    if (nodeInfo.progress.every((x) => x.completed) && task.shared.taskState === "Enabled") {
        return (
            <ClickableProgress onClick={onClick}>
                <ProgressCircle state="success" icon="check" onClick={onClick}>
                    up to date
                </ProgressCircle>
            </ClickableProgress>
        );
    }

    // at least one transformation is not completed - let's calculate total progress
    const totalItems = nodeInfo.progress.reduce((acc, current) => acc + current.global.total, 0);
    const totalProcessed = nodeInfo.progress.reduce((acc, current) => acc + current.global.processed, 0);

    const percentage = totalItems === 0 ? 1 : Math.floor((totalProcessed * 100) / totalItems) / 100;

    return (
        <ClickableProgress onClick={onClick}>
            <ProgressCircle state="running" icon={disabled ? "stop" : null} progress={percentage} onClick={onClick}>
                {disabled ? "Disabled" : "Running"}
            </ProgressCircle>
        </ClickableProgress>
    );
}

const taskNodeInfoKey = (
    task: OngoingTaskInfo,
    nodeInfo: OngoingReplicationProgressAwareTaskNodeInfo<OngoingTaskAbstractReplicationNodeInfoDetails>
) => {
    switch (task.shared.taskType) {
        case "PullReplicationAsHub":
            // since one hub can handle multiple sinks, we can't use (shard, nodeTag) for unique key
            // instead we use handlerId (which is random guid)
            return nodeInfo.details.handlerId;
        default:
            return nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
    }
};
