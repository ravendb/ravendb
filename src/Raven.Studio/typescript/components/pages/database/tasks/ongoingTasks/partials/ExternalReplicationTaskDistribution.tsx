import {
    OngoingReplicationProgressAwareTaskNodeInfo,
    OngoingTaskAbstractReplicationNodeInfoDetails,
    OngoingTaskExternalReplicationInfo,
    OngoingTaskInfo,
    OngoingTaskReplicationHubInfo,
    OngoingTaskReplicationSinkInfo,
} from "components/models/tasks";
import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import classNames from "classnames";
import { ProgressCircle } from "components/common/ProgressCircle";
import { ReplicationTaskProgressTooltip } from "components/pages/database/tasks/ongoingTasks/partials/ReplicationTaskProgressTooltip";
import { databaseLocationComparator, withPreventDefault } from "components/utils/common";
import { ErrorModal } from "components/pages/database/tasks/ongoingTasks/partials/ErrorModal";

interface ExternalReplicationTaskDistributionProps {
    task: OngoingTaskExternalReplicationInfo | OngoingTaskReplicationHubInfo | OngoingTaskReplicationSinkInfo;
}

interface ItemWithTooltipProps {
    nodeInfo: OngoingReplicationProgressAwareTaskNodeInfo<OngoingTaskAbstractReplicationNodeInfoDetails>;
    sharded: boolean;
    task: OngoingTaskExternalReplicationInfo | OngoingTaskReplicationHubInfo | OngoingTaskReplicationSinkInfo;
}

function ItemWithTooltip(props: ItemWithTooltipProps) {
    const { nodeInfo, sharded, task } = props;

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
    const [node, setNode] = useState<HTMLDivElement>();

    return (
        <div ref={setNode}>
            <DistributionItem loading={nodeInfo.status === "loading" || nodeInfo.status === "idle"} key={key}>
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
                <ExternalReplicationTaskProgress task={task} nodeInfo={nodeInfo} />
            </DistributionItem>
            {node &&
                (errorToDisplay ? (
                    <ErrorModal key="modal" toggleErrorModal={toggleErrorModal} error={errorToDisplay} />
                ) : (
                    <ReplicationTaskProgressTooltip
                        hasError={!!nodeInfo.details?.error}
                        toggleErrorModal={toggleErrorModal}
                        target={node}
                        progress={nodeInfo.progress}
                        status={nodeInfo.status}
                        lastAcceptedChangeVectorFromDestination={
                            nodeInfo.details?.lastAcceptedChangeVectorFromDestination
                        }
                        sourceDatabaseChangeVector={nodeInfo.details?.sourceDatabaseChangeVector}
                    />
                ))}
        </div>
    );
}

export function ExternalReplicationTaskDistribution(props: ExternalReplicationTaskDistributionProps) {
    const { task } = props;
    const sharded = task.nodesInfo.some((x) => x.location.shardNumber != null);

    const visibleNodes = task.nodesInfo.filter(
        (nodeInfo) =>
            nodeInfo.details && task.responsibleLocations.find((l) => databaseLocationComparator(l, nodeInfo.location))
    );

    const items = visibleNodes.map((nodeInfo) => {
        const key = taskNodeInfoKey(task, nodeInfo);

        return <ItemWithTooltip key={key} nodeInfo={nodeInfo} sharded={sharded} task={task} />;
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
}

export function ExternalReplicationTaskProgress(props: ExternalReplicationTaskProgressProps) {
    const { nodeInfo, task } = props;

    const disabled = task.shared.taskState === "Disabled";

    if (!nodeInfo.progress || nodeInfo.progress.length === 0) {
        return (
            <ProgressCircle icon={disabled ? "stop" : null} state="running">
                {disabled ? "Disabled" : "?"}
            </ProgressCircle>
        );
    }

    if (nodeInfo.progress.every((x) => x.completed) && task.shared.taskState === "Enabled") {
        return (
            <ProgressCircle state="success" icon="check">
                up to date
            </ProgressCircle>
        );
    }

    // at least one transformation is not completed - let's calculate total progress
    const totalItems = nodeInfo.progress.reduce((acc, current) => acc + current.global.total, 0);
    const totalProcessed = nodeInfo.progress.reduce((acc, current) => acc + current.global.processed, 0);

    const percentage = totalItems === 0 ? 1 : Math.floor((totalProcessed * 100) / totalItems) / 100;

    return (
        <ProgressCircle state="running" icon={disabled ? "stop" : null} progress={percentage}>
            {disabled ? "Disabled" : "Running"}
        </ProgressCircle>
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
