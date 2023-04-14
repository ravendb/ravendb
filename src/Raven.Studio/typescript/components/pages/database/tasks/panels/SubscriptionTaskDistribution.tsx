import React, { useState } from "react";
import { DistributionItem, DistributionLegend, LocationDistribution } from "../../../../common/LocationDistribution";
import classNames from "classnames";
import { OngoingTaskInfo, OngoingTaskNodeInfo, OngoingTaskSubscriptionInfo } from "../../../../models/tasks";
import { ProgressCircle } from "../../../../common/ProgressCircle";
import { Icon } from "components/common/Icon";

interface OngoingEtlTaskDistributionProps {
    task: OngoingTaskSubscriptionInfo;
}

export function SubscriptionTaskDistribution(props: OngoingEtlTaskDistributionProps) {
    const { task } = props;
    const sharded = task.nodesInfo.some((x) => x.location.shardNumber != null);

    const [uniqueTaskId] = useState(() => _.uniqueId("task-id"));

    const visibleNodes = task.nodesInfo.filter(
        (x) => x.status !== "success" || x.details.taskConnectionStatus !== "NotOnThisNode"
    );

    const items = (
        <>
            {visibleNodes.map((nodeInfo) => {
                const shard = (
                    <div className="top shard">
                        {nodeInfo.location.shardNumber != null && (
                            <>
                                <Icon icon="shard" className="me-1" />
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
                        loading={nodeInfo.status === "loading" || nodeInfo.status === "idle"}
                        id={id}
                        key={key}
                    >
                        {sharded && shard}
                        <div className={classNames("node", { top: !sharded })}>
                            {!sharded && <Icon icon="node" className="me-1" />}

                            {nodeInfo.location.nodeTag}
                        </div>
                        <div>{nodeInfo.status === "success" ? nodeInfo.details.taskConnectionStatus : ""}</div>
                        <div>{hasError ? <Icon icon="warning" color="danger" /> : "-"}</div>
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
                        <Icon icon="node" className="me-1" /> Node
                    </div>
                )}
                <div>
                    <Icon icon="connected" className="me-1" /> Status
                </div>
                <div>
                    <Icon icon="warning" className="me-1" /> Error
                </div>
                <div>
                    <Icon icon="" className="me-1" />
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
    const { nodeInfo } = props;

    if (nodeInfo.status === "failure") {
        return <ProgressCircle state="running" icon="warning" />;
    }

    //TODO: show clients count?

    return (
        <ProgressCircle state="running" icon="check">
            OK
        </ProgressCircle>
    );
}
/* TODO
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
 */

const taskNodeInfoKey = (nodeInfo: OngoingTaskNodeInfo) =>
    nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
