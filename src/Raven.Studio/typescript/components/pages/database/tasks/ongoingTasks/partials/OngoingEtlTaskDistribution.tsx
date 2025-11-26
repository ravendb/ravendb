import React, { useState } from "react";
import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import classNames from "classnames";
import { AnyEtlOngoingTaskInfo, OngoingEtlTaskNodeInfo, OngoingTaskInfo } from "components/models/tasks";
import { ProgressCircle } from "components/common/ProgressCircle";
import { OngoingEtlTaskProgressTooltip } from "../partials/OngoingEtlTaskProgressTooltip";
import { Icon } from "components/common/Icon";
import { databaseLocationComparator, withPreventDefault } from "components/utils/common";
import { ErrorModal } from "components/pages/database/tasks/ongoingTasks/partials/ErrorModal";

interface OngoingEtlTaskDistributionProps {
    task: AnyEtlOngoingTaskInfo;
    showPreview: (transformationName: string) => void;
}

interface ItemWithTooltipProps {
    nodeInfo: OngoingEtlTaskNodeInfo;
    sharded: boolean;
    task: AnyEtlOngoingTaskInfo;
    showPreview: (transformationName: string) => void;
}

function ItemWithTooltip(props: ItemWithTooltipProps) {
    const { nodeInfo, sharded, task, showPreview } = props;

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

    const key = taskNodeInfoKey(nodeInfo);
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
                    {hasError ? (
                        <a href="#" onClick={withPreventDefault(toggleErrorModal)}>
                            <Icon icon="warning" color="danger" margin="m-0" />
                        </a>
                    ) : (
                        "-"
                    )}
                </div>
                <OngoingEtlTaskProgress task={task} nodeInfo={nodeInfo} />
            </DistributionItem>
            {node &&
                (errorToDisplay ? (
                    <ErrorModal key="modal" toggleErrorModal={toggleErrorModal} error={errorToDisplay} />
                ) : (
                    <OngoingEtlTaskProgressTooltip
                        hasError={!!nodeInfo.details?.error}
                        toggleErrorModal={toggleErrorModal}
                        target={node}
                        progress={nodeInfo.etlProgress}
                        status={nodeInfo.status}
                        showPreview={showPreview}
                    />
                ))}
        </div>
    );
}

export function OngoingEtlTaskDistribution(props: OngoingEtlTaskDistributionProps) {
    const { task, showPreview } = props;
    const sharded = task.nodesInfo.some((x) => x.location.shardNumber != null);

    const visibleNodes = task.nodesInfo.filter(
        (nodeInfo) =>
            nodeInfo.details && task.responsibleLocations.find((l) => databaseLocationComparator(l, nodeInfo.location))
    );

    const items = visibleNodes.map((nodeInfo) => {
        const key = taskNodeInfoKey(nodeInfo);

        return (
            <ItemWithTooltip key={key} nodeInfo={nodeInfo} sharded={sharded} showPreview={showPreview} task={task} />
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

interface OngoingEtlTaskProgressProps {
    nodeInfo: OngoingEtlTaskNodeInfo;
    task: OngoingTaskInfo;
}

export function OngoingEtlTaskProgress(props: OngoingEtlTaskProgressProps) {
    const { nodeInfo, task } = props;

    const disabled = task.shared.taskState === "Disabled";

    if (!nodeInfo.etlProgress || nodeInfo.etlProgress.length === 0) {
        return (
            <ProgressCircle icon={disabled ? "stop" : null} state="running">
                {disabled ? "Disabled" : "?"}
            </ProgressCircle>
        );
    }

    if (nodeInfo.etlProgress.every((x) => x.completed) && task.shared.taskState === "Enabled") {
        return (
            <ProgressCircle state="success" icon="check">
                up to date
            </ProgressCircle>
        );
    }

    // at least one transformation is not completed - let's calculate total progress
    const totalItems = nodeInfo.etlProgress.reduce((acc, current) => acc + current.global.total, 0);
    const totalProcessed = nodeInfo.etlProgress.reduce((acc, current) => acc + current.global.processed, 0);

    const percentage = totalItems === 0 ? 1 : Math.floor((totalProcessed * 100) / totalItems) / 100;
    const anyDisabled = nodeInfo.etlProgress.some((x) => x.disabled);

    return (
        <ProgressCircle state="running" icon={anyDisabled ? "stop" : null} progress={percentage}>
            {anyDisabled ? "Disabled" : "Running"}
        </ProgressCircle>
    );
}

const taskNodeInfoKey = (nodeInfo: OngoingEtlTaskNodeInfo) =>
    nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
