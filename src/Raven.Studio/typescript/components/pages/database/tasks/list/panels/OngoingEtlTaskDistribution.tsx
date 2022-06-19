import React, { useState } from "react";
import { DistributionItem, DistributionLegend, LocationDistribution } from "../../../../../common/LocationDistribution";
import classNames from "classnames";
import { AnyEtlOngoingTaskInfo, OngoingEtlTaskNodeInfo, OngoingTaskInfo } from "../../../../../models/tasks";
import { ProgressCircle } from "../../../../../common/ProgressCircle";
import { OngoingEtlTaskProgressTooltip } from "../OngoingEtlTaskProgressTooltip";

interface OngoingEtlTaskDistributionProps {
    task: AnyEtlOngoingTaskInfo;
    showPreview: (transformationName: string) => void;
}

export function OngoingEtlTaskDistribution(props: OngoingEtlTaskDistributionProps) {
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
                        <OngoingEtlTaskProgress task={task} nodeInfo={nodeInfo} />
                        <OngoingEtlTaskProgressTooltip
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

interface OngoingEtlTaskProgressProps {
    nodeInfo: OngoingEtlTaskNodeInfo;
    task: OngoingTaskInfo;
}

export function OngoingEtlTaskProgress(props: OngoingEtlTaskProgressProps) {
    const { nodeInfo, task } = props;
    if (!nodeInfo.etlProgress) {
        return <ProgressCircle state="running" />;
    }

    if (nodeInfo.etlProgress.every((x) => x.completed) && task.shared.taskState === "Enabled") {
        return (
            <ProgressCircle state="success" icon="icon-check">
                up to date
            </ProgressCircle>
        );
    }

    // at least one transformation is not completed - let's calculate total progress
    const totalItems = nodeInfo.etlProgress.reduce((acc, current) => acc + current.global.total, 0);
    const totalProcessed = nodeInfo.etlProgress.reduce((acc, current) => acc + current.global.processed, 0);

    const percentage = Math.floor((totalProcessed * 100) / totalItems) / 100;
    const anyDisabled = nodeInfo.etlProgress.some((x) => x.disabled);

    return (
        <ProgressCircle state="running" icon={anyDisabled ? "icon-stop" : null} progress={percentage}>
            {anyDisabled ? "Disabled" : "Running"}
        </ProgressCircle>
    );
}

const taskNodeInfoKey = (nodeInfo: OngoingEtlTaskNodeInfo) =>
    nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
