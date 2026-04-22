import React, { useState } from "react";
import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import classNames from "classnames";
import { AnyEtlOngoingTaskInfo, OngoingEtlTaskNodeInfo, OngoingTaskInfo } from "components/models/tasks";
import { ProgressCircle } from "components/common/ProgressCircle";
import { OngoingEtlTaskProgressTooltip } from "../partials/OngoingEtlTaskProgressTooltip";
import { Icon } from "components/common/Icon";
import { databaseLocationComparator, withPreventDefault } from "components/utils/common";
import { ErrorModal } from "components/pages/database/tasks/ongoingTasks/partials/ErrorModal";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import copyToClipboard from "common/copyToClipboard";

interface OngoingEtlTaskDistributionProps {
    task: AnyEtlOngoingTaskInfo;
    showPreview: (transformationName: string) => void;
}

interface ItemWithTooltipProps {
    nodeInfo: OngoingEtlTaskNodeInfo;
    sharded: boolean;
    task: AnyEtlOngoingTaskInfo;
    showPreview: (transformationName: string) => void;
    expectsTxId: boolean;
    txIdScripts: string[];
    singleScript: boolean;
}

function buildTxIdCells(
    expectsTxId: boolean,
    txIdScripts: string[],
    singleScript: boolean,
    nodeInfo: OngoingEtlTaskNodeInfo
): React.JSX.Element[] {
    if (!expectsTxId) {
        return [];
    }

    const hasProgress = !!nodeInfo.etlProgress?.length;

    const loadingDiv = (key: string) => (
        <div key={key} className="d-flex align-items-center justify-content-center gap-1 text-muted">
            <Spinner animation="border" size="sm" />
            <small>Loading...</small>
        </div>
    );

    const txIdDiv = (key: string, txId: string | undefined, noTopBorder = false) => {
        const borderClass = noTopBorder ? "no-top-border" : undefined;
        if (!txId) {
            return hasProgress ? (
                <div key={key} className={borderClass}>
                    -
                </div>
            ) : (
                loadingDiv(key)
            );
        }
        return (
            <div
                key={key}
                className={classNames(
                    "d-flex align-items-center justify-content-center gap-1 overflow-hidden",
                    borderClass
                )}
            >
                <span className="text-truncate" title={txId}>
                    {txId}
                </span>
                <Button
                    variant="link"
                    size="xs"
                    className="p-0 flex-shrink-0"
                    onClick={() => copyToClipboard.copy(txId, "Transactional Id was copied to clipboard.")}
                    title="Copy to clipboard"
                >
                    <Icon icon="copy" margin="m-0" />
                </Button>
            </div>
        );
    };

    if (txIdScripts.length === 0) {
        return [loadingDiv("txid-loading")];
    }

    if (singleScript) {
        const txId = nodeInfo.etlProgress?.find((ep) => ep.transformationName === txIdScripts[0])?.transactionalId;
        return [txIdDiv(`txid-${txIdScripts[0]}`, txId)];
    }

    return [
        <div key="txid-header"></div>,
        ...txIdScripts.map((script) => {
            const txId = nodeInfo.etlProgress?.find((ep) => ep.transformationName === script)?.transactionalId;
            return txIdDiv(`txid-${script}`, txId, true);
        }),
    ];
}

function ItemWithTooltip(props: ItemWithTooltipProps) {
    const { nodeInfo, sharded, task, showPreview, expectsTxId, txIdScripts, singleScript } = props;

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

    const txIdCells = buildTxIdCells(expectsTxId, txIdScripts, singleScript, nodeInfo);

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
                {txIdCells}
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

    const expectsTxId = task.shared.taskType === "KafkaQueueEtl";

    const txIdScripts: string[] = expectsTxId
        ? Array.from(
              visibleNodes.reduce((acc, nodeInfo) => {
                  nodeInfo.etlProgress?.forEach((ep) => {
                      if (ep.transactionalId) {
                          acc.add(ep.transformationName);
                      }
                  });
                  return acc;
              }, new Set<string>())
          )
        : [];

    const singleScript = txIdScripts.length === 1;

    const items = visibleNodes.map((nodeInfo) => {
        const key = taskNodeInfoKey(nodeInfo);

        return (
            <ItemWithTooltip
                key={key}
                nodeInfo={nodeInfo}
                sharded={sharded}
                showPreview={showPreview}
                task={task}
                expectsTxId={expectsTxId}
                txIdScripts={txIdScripts}
                singleScript={singleScript}
            />
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
                    {expectsTxId && txIdScripts.length <= 1 && (
                        <div className="pe-1">
                            <Icon icon="identities" /> Transactional ID
                        </div>
                    )}
                    {expectsTxId && txIdScripts.length > 1 && (
                        <>
                            <div className="pe-1">
                                <Icon icon="identities" /> Transactional IDs
                            </div>
                            {txIdScripts.map((script) => (
                                <div key={script} className="no-top-border ps-3">
                                    {script}
                                </div>
                            ))}
                        </>
                    )}
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
