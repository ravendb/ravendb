import React, { useState } from "react";
import { DistributionItem, DistributionLegend, LocationDistribution } from "../../../../../common/LocationDistribution";
import classNames from "classnames";
import {
    AnyEtlOngoingTaskInfo,
    OngoingEtlTaskNodeInfo,
    OngoingTaskElasticSearchEtlInfo,
    OngoingTaskInfo,
    OngoingTaskNodeInfo,
    OngoingTaskOlapEtlInfo,
    OngoingTaskRavenEtlInfo,
    OngoingTaskSqlEtlInfo,
    OngoingTaskSubscriptionInfo,
    OngoingTaskSubscriptionNodeInfoDetails,
} from "../../../../../models/tasks";
import { ProgressCircle } from "../../../../../common/ProgressCircle";
import { OngoingEtlTaskProgressTooltip } from "../OngoingEtlTaskProgressTooltip";
import { OngoingEtlTaskProgress } from "./OngoingEtlTaskDistribution";
import { PopoverWithHover } from "../../../../../common/PopoverWithHover";

interface OngoingEtlTaskDistributionProps {
    task: OngoingTaskSubscriptionInfo;
}

export function SubscriptionTaskDistribution(props: OngoingEtlTaskDistributionProps) {
    const { task } = props;
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
                        <SubscriptionTaskProgress task={task} nodeInfo={nodeInfo} />
                        {/* TODO: <SubscriptionTaskProgressTooltip target={id} nodeInfo={nodeInfo} task={task} />*/}
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

interface SubscriptionTaskProgressProps {
    nodeInfo: OngoingTaskNodeInfo;
    task: OngoingTaskInfo;
}

export function SubscriptionTaskProgress(props: SubscriptionTaskProgressProps) {
    const { nodeInfo, task } = props;

    //TODO: show clients count?

    return (
        <ProgressCircle state="running" icon="icon-check">
            OK
        </ProgressCircle>
    );
}

interface SubscriptionTaskProgressTooltipProps {
    target: string;
    nodeInfo: OngoingTaskNodeInfo;
    task: OngoingTaskInfo;
}

function SubscriptionTaskProgressTooltip(props: SubscriptionTaskProgressTooltipProps) {
    const { target } = props;
    return (
        <PopoverWithHover rounded target={target} placement="top" delay={100}>
            <div className="ongoing-tasks-details-tooltip">
                <ul>
                    <li>include clients details (ip port, Strategy, worker id?)</li>
                </ul>
            </div>
        </PopoverWithHover>
    );
}

const taskNodeInfoKey = (nodeInfo: OngoingTaskNodeInfo) =>
    nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
