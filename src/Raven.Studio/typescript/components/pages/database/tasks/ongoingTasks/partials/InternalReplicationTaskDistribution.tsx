import {
    OngoingInternalReplicationNodeInfo,
    OngoingTaskNodeInternalReplicationProgressDetails,
} from "components/models/tasks";
import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import classNames from "classnames";
import { ProgressCircle } from "components/common/ProgressCircle";
import { ReplicationTaskProgressTooltip } from "components/pages/database/tasks/ongoingTasks/partials/ReplicationTaskProgressTooltip";
import { withPreventDefault } from "components/utils/common";
import { ErrorModal } from "components/pages/database/tasks/ongoingTasks/partials/ErrorModal";
import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useExactProgress } from "components/pages/database/tasks/ongoingTasks/partials/useExactProgress";
import { mapInternalReplicationProgress } from "components/pages/database/tasks/ongoingTasks/partials/OngoingTasksReducer";

interface ItemWithTooltipProps {
    nodeInfo: Omit<OngoingInternalReplicationNodeInfo, "progress">;
    sharded: boolean;
    progress: OngoingTaskNodeInternalReplicationProgressDetails;
}

function ItemWithTooltip(props: ItemWithTooltipProps) {
    const { nodeInfo, sharded } = props;

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
        setErrorToDisplay((error) => (error ? null : nodeInfo.error));
    };

    const [node, setNode] = useState<HTMLDivElement>();

    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { progress, calculateExact, isCalculatingExact } = useExactProgress(props.progress, async () => {
        const { Results } = await tasksService.getInternalReplicationProgress(databaseName, nodeInfo.location, true);
        const destinationProgress = Results.find((x) => x.DestinationNodeTag === props.progress?.destinationNodeTag);
        return destinationProgress && mapInternalReplicationProgress(destinationProgress);
    });

    const hasError = nodeInfo.status === "failure";
    return (
        <div ref={setNode}>
            <DistributionItem loading={nodeInfo.status === "loading" || nodeInfo.status === "idle"}>
                {sharded && shard}
                <div className={classNames("node", { top: !sharded })}>
                    {!sharded && <Icon icon="node" />}
                    {nodeInfo.location.nodeTag} &gt; {progress?.destinationNodeTag ?? "?"}
                </div>
                <div>{progress?.lastDatabaseEtag ? progress.lastDatabaseEtag.toLocaleString() : "-"}</div>
                <div>{progress?.lastSentEtag ? progress.lastSentEtag.toLocaleString() : "-"}</div>
                <div>
                    {hasError ? (
                        <a href="#" onClick={withPreventDefault(toggleErrorModal)}>
                            <Icon icon="warning" color="danger" margin="m-0" />
                        </a>
                    ) : (
                        "-"
                    )}
                </div>
                <InternalReplicationTaskProgress progress={progress} />
            </DistributionItem>
            {node &&
                (errorToDisplay ? (
                    <ErrorModal key="modal" toggleErrorModal={toggleErrorModal} error={errorToDisplay} />
                ) : (
                    <ReplicationTaskProgressTooltip
                        hasError={nodeInfo.status === "failure"}
                        toggleErrorModal={toggleErrorModal}
                        target={node}
                        progress={progress ? [progress] : []}
                        status={nodeInfo.status}
                        lastAcceptedChangeVectorFromDestination={progress?.lastAcceptedChangeVectorFromDestination}
                        sourceDatabaseChangeVector={progress?.sourceDatabaseChangeVector}
                        calculateExact={calculateExact}
                        isCalculatingExact={isCalculatingExact}
                    />
                ))}
        </div>
    );
}

interface InternalReplicationTaskDistributionProps {
    data: OngoingInternalReplicationNodeInfo[];
}

export function InternalReplicationTaskDistribution(props: InternalReplicationTaskDistributionProps) {
    const { data } = props;

    const sharded = data.some((x) => x.location.shardNumber != null);

    const items = data.flatMap((nodeInfo) => {
        if (!nodeInfo.progress.length) {
            const key = taskNodeInfoKey(nodeInfo) + "->" + "?";
            return <ItemWithTooltip key={key} nodeInfo={nodeInfo} sharded={sharded} progress={null} />;
        }

        return nodeInfo.progress.map((progress) => {
            const key = taskNodeInfoKey(nodeInfo) + "->" + progress.destinationNodeTag;
            return <ItemWithTooltip key={key} nodeInfo={nodeInfo} progress={progress} sharded={sharded} />;
        });
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

interface InternalReplicationTaskProgressProps {
    progress: OngoingTaskNodeInternalReplicationProgressDetails;
}

export function InternalReplicationTaskProgress(props: InternalReplicationTaskProgressProps) {
    const { progress } = props;

    if (!progress) {
        return <ProgressCircle state="running" />;
    }

    if (progress.completed) {
        return (
            <ProgressCircle state="success" icon="check">
                up to date
            </ProgressCircle>
        );
    }

    // at least one transformation is not completed - let's calculate total progress
    const totalItems = progress.global.total;
    const totalProcessed = progress.global.processed;

    const percentage = Math.floor((totalProcessed * 100) / totalItems) / 100;

    return (
        <ProgressCircle state="running" icon={null} progress={percentage}>
            Running
        </ProgressCircle>
    );
}

const taskNodeInfoKey = (nodeInfo: OngoingInternalReplicationNodeInfo) => {
    return nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
};
