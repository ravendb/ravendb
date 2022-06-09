import React, { useState } from "react";
import { DistributionItem, DistributionLegend, LocationDistribution } from "../../../../../common/LocationDistribution";
import classNames from "classnames";
import { OngoingTaskInfo, OngoingTaskNodeInfo } from "../../../../../models/tasks";
import { ProgressCircle } from "../../../../../common/ProgressCircle";
import { OngoingTaskProgressTooltip } from "../OngoingTaskProgressTooltip";

interface OngoingTaskDistributionProps {
    task: OngoingTaskInfo;
    showPreview: (transformationName: string) => void;
}

export function OngoingTaskDistribution(props: OngoingTaskDistributionProps) {
    const { task, showPreview } = props;
    const sharded = task.nodesInfo.some((x) => x.location.shardNumber != null);

    const [uniqueTaskId] = useState(() => _.uniqueId("task-id"));

    const items = (
        <>
            {task.nodesInfo.map((nodeInfo) => {
                const shard = (
                    <div className="top shard">
                        {nodeInfo.location.shardNumber != null && (
                            <>
                                <i className="icon-shard" />
                                {nodeInfo.location.shardNumber}
                            </>
                        )}
                    </div>
                );

                const key = taskNodeInfoKey(nodeInfo);
                const id = uniqueTaskId + key;

                const hasError = !!nodeInfo.details?.error;

                return (
                    <DistributionItem
                        loading={nodeInfo.status === "loading" || nodeInfo.status === "notLoaded"}
                        id={id}
                        key={key}
                    >
                        {sharded && shard}
                        <div className={classNames("node", { top: !sharded })}>
                            {!sharded && <i className="icon-node"></i>}

                            {nodeInfo.location.nodeTag}
                        </div>
                        <div>{nodeInfo.status === "loaded" ? nodeInfo.details.taskConnectionStatus : ""}</div>
                        <div>{hasError ? "error" : "-"}</div>
                        <OngoingTaskProgress nodeInfo={nodeInfo} />
                        <OngoingTaskProgressTooltip
                            target={id}
                            nodeInfo={nodeInfo}
                            task={task}
                            showPreview={showPreview}
                        />
                    </DistributionItem>
                );
            })}
        </>
    );

    return (
        <LocationDistribution>
            <DistributionLegend>
                <div className="top"></div>
                {sharded && (
                    <div className="node">
                        <i className="icon-node" /> Node
                    </div>
                )}
                <div>
                    <i className="icon-connected" /> Status
                </div>
                <div>
                    <i className="icon-warning" /> Error
                </div>
                <div>
                    <i />
                    Status
                </div>
            </DistributionLegend>
            {items}
        </LocationDistribution>
    );
}

interface OngoingTaskProgressProps {
    nodeInfo: OngoingTaskNodeInfo;
}

export function OngoingTaskProgress(props: OngoingTaskProgressProps) {
    const { nodeInfo } = props;
    if (!nodeInfo.progress) {
        return <ProgressCircle state="running" />;
    }

    if (nodeInfo.progress.every((x) => x.completed)) {
        return (
            <ProgressCircle state="success" icon="icon-check">
                up to date
            </ProgressCircle>
        );
    }

    // at least one transformation is not completed - let's calculate total progress
    const totalItems = nodeInfo.progress.reduce((acc, current) => acc + current.global.total, 0);
    const totalProcessed = nodeInfo.progress.reduce((acc, current) => acc + current.global.processed, 0);

    const percentage = Math.floor((totalProcessed * 100) / totalItems) / 100;
    const anyDisabled = nodeInfo.progress.some((x) => x.disabled);

    return (
        <ProgressCircle state="running" icon={anyDisabled ? "icon-stop" : null} progress={percentage}>
            {anyDisabled ? "Disabled" : "Running"}
        </ProgressCircle>
    );
}

const taskNodeInfoKey = (nodeInfo: OngoingTaskNodeInfo) =>
    nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
