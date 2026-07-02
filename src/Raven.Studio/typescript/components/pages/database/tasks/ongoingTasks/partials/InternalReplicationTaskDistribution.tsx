import {
    OngoingInternalReplicationNodeInfo,
    OngoingTaskNodeInternalReplicationProgressDetails,
    OngoingReplicationProgressAwareTaskNodeInfo,
    OngoingTaskAbstractReplicationNodeInfoDetails,
} from "components/models/tasks";
import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import { Icon } from "components/common/Icon";
import React, { useId, useState } from "react";
import classNames from "classnames";
import { ProgressCircle } from "components/common/ProgressCircle";
import { ReplicationProgressDetailsSheet } from "components/pages/database/tasks/ongoingTasks/partials/ReplicationProgressDetailsSheet";
import { withPreventDefault } from "components/utils/common";
import { ErrorModal } from "components/pages/database/tasks/ongoingTasks/partials/ErrorModal";
import { useViewSheet } from "components/common/splitView/ViewSheet";

interface ItemWithTooltipProps {
    nodeInfo: Omit<OngoingInternalReplicationNodeInfo, "progress">;
    sharded: boolean;
    progress: OngoingTaskNodeInternalReplicationProgressDetails;
    allSyntheticNodes: OngoingReplicationProgressAwareTaskNodeInfo<OngoingTaskAbstractReplicationNodeInfoDetails>[];
    syntheticNodeIndex: number;
    isActive: boolean;
    setActiveNodeIndex: (index: number | null) => void;
    ownerId: string;
}

function ItemWithTooltip(props: ItemWithTooltipProps) {
    const {
        nodeInfo,
        sharded,
        progress,
        allSyntheticNodes,
        syntheticNodeIndex,
        isActive,
        setActiveNodeIndex,
        ownerId,
    } = props;

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

    const hasError = nodeInfo.status === "failure";

    const { open } = useViewSheet();

    const openProgressSheet = () => {
        setActiveNodeIndex(syntheticNodeIndex);
        open({
            ownerId,
            component: (
                <ReplicationProgressDetailsSheet
                    key={ownerId}
                    taskType="Internal Replication"
                    allNodes={allSyntheticNodes}
                    initialNodeIndex={syntheticNodeIndex}
                    onNodeChange={setActiveNodeIndex}
                />
            ),
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
                className={classNames({ active: isActive })}
            >
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
                <InternalReplicationTaskProgress
                    progress={progress}
                    onClick={canOpenSheet ? openProgressSheet : undefined}
                />
            </DistributionItem>
            {errorToDisplay && <ErrorModal key="modal" toggleErrorModal={toggleErrorModal} error={errorToDisplay} />}
        </div>
    );
}

interface InternalReplicationTaskDistributionProps {
    data: OngoingInternalReplicationNodeInfo[];
}

function buildSyntheticNode(
    nodeInfo: Omit<OngoingInternalReplicationNodeInfo, "progress">,
    progress: OngoingTaskNodeInternalReplicationProgressDetails | null,
    label: string
): OngoingReplicationProgressAwareTaskNodeInfo<OngoingTaskAbstractReplicationNodeInfoDetails> {
    return {
        location: { nodeTag: label, shardNumber: nodeInfo.location.shardNumber },
        status: nodeInfo.status,
        details: {
            sourceDatabaseChangeVector: progress?.sourceDatabaseChangeVector ?? null,
            lastAcceptedChangeVectorFromDestination: progress?.lastAcceptedChangeVectorFromDestination ?? null,
            error: nodeInfo.error ?? null,
            taskConnectionStatus: null,
            responsibleNode: nodeInfo.location.nodeTag,
            handlerId: null,
            fromToString: label,
            lastSentEtag: progress?.lastSentEtag ?? 0,
            lastDatabaseEtag: progress?.lastDatabaseEtag ?? 0,
        } as OngoingTaskAbstractReplicationNodeInfoDetails,
        progress: progress ? [progress] : [],
    };
}

export function InternalReplicationTaskDistribution(props: InternalReplicationTaskDistributionProps) {
    const { data } = props;

    const sharded = data.some((x) => x.location.shardNumber != null);
    const [activeNodeIndex, setActiveNodeIndex] = useState<number | null>(null);
    const ownerId = useId();
    const { activeSheetOwnerId } = useViewSheet();

    // Build a flat list of (nodeInfo, progress) pairs with synthetic nodes for the sheet
    interface InternalEntry {
        nodeInfo: Omit<OngoingInternalReplicationNodeInfo, "progress">;
        progress: OngoingTaskNodeInternalReplicationProgressDetails | null;
        key: string;
        syntheticNode: OngoingReplicationProgressAwareTaskNodeInfo<OngoingTaskAbstractReplicationNodeInfoDetails>;
    }

    const entries: InternalEntry[] = data.flatMap((nodeInfo) => {
        if (!nodeInfo.progress.length) {
            const label = `${nodeInfo.location.nodeTag} → ?`;
            return [
                {
                    nodeInfo,
                    progress: null,
                    key: taskNodeInfoKey(nodeInfo) + "->?",
                    syntheticNode: buildSyntheticNode(nodeInfo, null, label),
                },
            ];
        }
        return nodeInfo.progress.map((progress) => {
            const label = `${nodeInfo.location.nodeTag} → ${progress.destinationNodeTag}`;
            return {
                nodeInfo,
                progress,
                key: taskNodeInfoKey(nodeInfo) + "->" + progress.destinationNodeTag,
                syntheticNode: buildSyntheticNode(nodeInfo, progress, label),
            };
        });
    });

    const allSyntheticNodes = entries.map((e) => e.syntheticNode);

    const items = entries.map((entry, index) => (
        <ItemWithTooltip
            key={entry.key}
            nodeInfo={entry.nodeInfo}
            sharded={sharded}
            progress={entry.progress}
            allSyntheticNodes={allSyntheticNodes}
            syntheticNodeIndex={index}
            isActive={activeSheetOwnerId === ownerId && activeNodeIndex === index}
            setActiveNodeIndex={setActiveNodeIndex}
            ownerId={ownerId}
        />
    ));

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
    onClick?: () => void;
}

export function InternalReplicationTaskProgress(props: InternalReplicationTaskProgressProps) {
    const { progress, onClick } = props;

    if (!progress) {
        return <ProgressCircle state="running" onClick={onClick} />;
    }

    if (progress.completed) {
        return (
            <ProgressCircle state="success" icon="check" onClick={onClick}>
                up to date
            </ProgressCircle>
        );
    }

    // at least one transformation is not completed - let's calculate total progress
    const totalItems = progress.global.total;
    const totalProcessed = progress.global.processed;

    const percentage = Math.floor((totalProcessed * 100) / totalItems) / 100;

    return (
        <ProgressCircle state="running" icon={null} progress={percentage} onClick={onClick}>
            Running
        </ProgressCircle>
    );
}

const taskNodeInfoKey = (nodeInfo: OngoingInternalReplicationNodeInfo) => {
    return nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
};
