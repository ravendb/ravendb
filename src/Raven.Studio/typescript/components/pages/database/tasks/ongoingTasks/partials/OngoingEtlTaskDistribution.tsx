import { useState } from "react";
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

interface TxIdLayout {
    scripts: string[];
    singleScript: boolean;
}

interface ItemWithTooltipProps {
    nodeInfo: OngoingEtlTaskNodeInfo;
    sharded: boolean;
    task: AnyEtlOngoingTaskInfo;
    showPreview: (transformationName: string) => void;
    txIdLayout: TxIdLayout | null;
}

interface TransactionalIdCellProps {
    txId: string | undefined;
    hasProgress: boolean;
    noTopBorder?: boolean;
}

function TransactionalIdCell({ txId, hasProgress, noTopBorder = false }: TransactionalIdCellProps) {
    const borderClass = noTopBorder ? "border-top-0" : undefined;

    if (!txId) {
        return hasProgress ? (
            <div className={borderClass}>-</div>
        ) : (
            <div className="d-flex align-items-center justify-content-center gap-1 text-muted">
                <Spinner animation="border" size="sm" />
                <small>Loading...</small>
            </div>
        );
    }

    return (
        <div
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
}

interface TransactionalIdCellsProps {
    txIdLayout: TxIdLayout;
    nodeInfo: OngoingEtlTaskNodeInfo;
}

function TransactionalIdCells({ txIdLayout, nodeInfo }: TransactionalIdCellsProps) {
    const { scripts, singleScript } = txIdLayout;
    const hasProgress = !!nodeInfo.etlProgress?.length;

    if (scripts.length === 0) {
        return (
            <div className="d-flex align-items-center justify-content-center gap-1 text-muted">
                <Spinner animation="border" size="sm" />
                <small>Loading...</small>
            </div>
        );
    }

    if (singleScript) {
        const txId = nodeInfo.etlProgress?.find((ep) => ep.transformationName === scripts[0])?.transactionalId;
        return <TransactionalIdCell txId={txId} hasProgress={hasProgress} />;
    }

    return (
        <>
            <div />
            {scripts.map((script) => {
                const txId = nodeInfo.etlProgress?.find((ep) => ep.transformationName === script)?.transactionalId;
                return <TransactionalIdCell key={script} txId={txId} hasProgress={hasProgress} noTopBorder />;
            })}
        </>
    );
}

interface TransactionalIdLegendProps {
    txIdLayout: TxIdLayout;
}

function TransactionalIdLegend({ txIdLayout }: TransactionalIdLegendProps) {
    const { scripts } = txIdLayout;

    if (scripts.length <= 1) {
        return (
            <div className="pe-1">
                <Icon icon="identities" /> Transactional ID
            </div>
        );
    }

    return (
        <>
            <div className="pe-1">
                <Icon icon="identities" /> Transactional IDs
            </div>
            {scripts.map((script) => (
                <div key={script} className="border-top-0 ps-3">
                    {script}
                </div>
            ))}
        </>
    );
}

function ItemWithTooltip(props: ItemWithTooltipProps) {
    const { nodeInfo, sharded, task, showPreview, txIdLayout } = props;

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
                {txIdLayout && <TransactionalIdCells txIdLayout={txIdLayout} nodeInfo={nodeInfo} />}
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

    const txIdLayout: TxIdLayout | null = expectsTxId
        ? { scripts: txIdScripts, singleScript: txIdScripts.length === 1 }
        : null;

    const items = visibleNodes.map((nodeInfo) => {
        const key = taskNodeInfoKey(nodeInfo);

        return (
            <ItemWithTooltip
                key={key}
                nodeInfo={nodeInfo}
                sharded={sharded}
                showPreview={showPreview}
                task={task}
                txIdLayout={txIdLayout}
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
                    {txIdLayout && <TransactionalIdLegend txIdLayout={txIdLayout} />}
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
