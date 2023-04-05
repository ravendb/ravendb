import React, { useState } from "react";
import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import classNames from "classnames";
import {
    OngoingSubscriptionTaskNodeInfo,
    OngoingTaskInfo,
    OngoingTaskNodeInfo,
    OngoingTaskSubscriptionInfo,
} from "components/models/tasks";
import { PopoverWithHover } from "components/common/PopoverWithHover";
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
                    <Icon icon="icon-shard" />
                    {nodeInfo.location.shardNumber}
                </>
            )}
        </div>
    );

    const hasError = !!nodeInfo.details?.error;
    const [node, setNode] = useState<HTMLDivElement>();

    return (
        <div ref={setNode}>
            <DistributionItem loading={nodeInfo.status === "loading" || nodeInfo.status === "idle"}>
                {sharded && shard}
                <div className={classNames("node", { top: !sharded })}>
                    {!sharded && <i className="icon-node"></i>}

                    {nodeInfo.location.nodeTag}
                </div>
                <div>{nodeInfo.status === "success" ? nodeInfo.details.taskConnectionStatus : ""}</div>
                <div>{hasError ? <i className="icon-warning text-danger" /> : "-"}</div>
                <SubscriptionTaskProgress task={task} nodeInfo={nodeInfo} />
                {node && <SubscriptionTaskProgressTooltip target={node} nodeInfo={nodeInfo} task={task} />}
            </DistributionItem>
        </div>
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
                            <i className="icon-node" /> Node
                        </div>
                    )}
                    <div>
                        <Icon icon="icon-connected" /> Status
                    </div>
                    <div>
                        <Icon icon="icon-warning" /> Error
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

    //TODO: show clients count?

    return (
        <ProgressCircle state="running" icon="check">
            OK
        </ProgressCircle>
    );
}

interface SubscriptionTaskProgressTooltipProps {
    target: HTMLElement;
    nodeInfo: OngoingTaskNodeInfo;
    task: OngoingTaskInfo;
}

function SubscriptionTaskProgressTooltip(props: SubscriptionTaskProgressTooltipProps) {
    const { target } = props;
    return (
        <PopoverWithHover rounded="true" target={target} placement="top">
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
