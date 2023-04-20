import React from "react";
import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import classNames from "classnames";
import {
    OngoingSubscriptionTaskNodeInfo,
    OngoingTaskInfo,
    OngoingTaskNodeInfo,
    OngoingTaskSubscriptionInfo,
} from "components/models/tasks";
import { ProgressCircle } from "components/common/ProgressCircle";
import { Icon } from "components/common/Icon";

interface OngoingEtlTaskDistributionProps {
    task: OngoingTaskSubscriptionInfo;
}

interface ItemWithTooltipProps {
    nodeInfo: OngoingSubscriptionTaskNodeInfo;
    sharded: boolean;
    task: OngoingTaskSubscriptionInfo;
}

function ItemWithTooltip(props: ItemWithTooltipProps) {
    const { nodeInfo, task, sharded } = props;

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

    const hasError = !!nodeInfo.details?.error;

    return (
        <DistributionItem loading={nodeInfo.status === "loading" || nodeInfo.status === "idle"}>
            {sharded && shard}
            <div className={classNames("node", { top: !sharded })}>
                {!sharded && <Icon icon="node" />}

                {nodeInfo.location.nodeTag}
            </div>
            <div>{nodeInfo.status === "success" ? nodeInfo.details.taskConnectionStatus : ""}</div>
            <div>{hasError ? <Icon icon="warning" className="text-danger" /> : "-"}</div>
            <SubscriptionTaskProgress task={task} nodeInfo={nodeInfo} />
        </DistributionItem>
    );
}

export function SubscriptionTaskDistribution(props: OngoingEtlTaskDistributionProps) {
    const { task } = props;
    const sharded = task.nodesInfo.some((x) => x.location.shardNumber != null);

    const visibleNodes = task.nodesInfo.filter(
        (x) => x.status !== "success" || x.details.taskConnectionStatus !== "NotOnThisNode"
    );

    const items = (
        <>
            {visibleNodes.map((nodeInfo) => {
                const key = taskNodeInfoKey(nodeInfo);
                return <ItemWithTooltip key={key} nodeInfo={nodeInfo} sharded={sharded} task={task} />;
            })}
        </>
    );

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
                        <Icon icon="warning" /> Error
                    </div>
                    <div>
                        <i />
                        Status
                    </div>
                </DistributionLegend>
                {items}
            </LocationDistribution>
        </div>
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

    return (
        <ProgressCircle state="running" icon="check">
            OK
        </ProgressCircle>
    );
}

const taskNodeInfoKey = (nodeInfo: OngoingTaskNodeInfo) =>
    nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
