import { useState } from "react";
import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import classNames from "classnames";
import { AnyEtlOngoingTaskInfo, OngoingEtlTaskNodeInfo, OngoingTaskInfo } from "components/models/tasks";
import { ProgressCircle } from "components/common/ProgressCircle";
import { OngoingEtlTaskProgressTooltip } from "../partials/OngoingEtlTaskProgressTooltip";
import { Icon } from "components/common/Icon";
import { databaseLocationComparator } from "components/utils/common";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useAppUrls } from "hooks/useAppUrls";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { getPopoverMessageForTaskHealth, getTaskErrorCount } from "../panels/etlPanelUtils";
import { useServices } from "hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import EtlErrorDetailsSheet from "components/pages/database/tasks/tasksErrors/partials/EtlErrorDetailsSheet";
import {
    EtlHealthStatus,
    flattenAllTasksErrors,
    getTaskHealthStatus,
    getTasksWithErrors,
    healthStatusToBadge,
} from "components/pages/database/tasks/tasksErrors/utils/tasksErrorsUtils";
import genUtils from "common/generalUtils";
import moment from "moment";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;
import Spinner from "react-bootstrap/Spinner";
import copyToClipboard from "common/copyToClipboard";

interface OngoingEtlTaskDistributionProps {
    task: AnyEtlOngoingTaskInfo;
    showPreview: (transformationName: string) => void;
    etlStats?: EtlTaskStats[];
}

interface TxIdLayout {
    scripts: string[];
    isSingleScript: boolean;
}

interface ItemWithTooltipProps {
    nodeInfo: OngoingEtlTaskNodeInfo;
    sharded: boolean;
    task: AnyEtlOngoingTaskInfo;
    showPreview: (transformationName: string) => void;
    etlStats?: EtlTaskStats[];
    txIdLayout: TxIdLayout | null;
}

interface ConnectionStatusCellProps {
    status: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskConnectionStatus;
    processNames: string[];
    location: databaseLocationSpecifier;
    toggleErrorModal: () => void;
    hasErrors: boolean;
    nextBatchRetryTime?: string;
    onRetrySuccess?: () => Promise<unknown>;
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
    const { scripts, isSingleScript } = txIdLayout;
    const hasProgress = !!nodeInfo.etlProgress?.length;

    if (scripts.length === 0) {
        return (
            <div className="d-flex align-items-center justify-content-center gap-1 text-muted">
                <Spinner animation="border" size="sm" />
                <small>Loading...</small>
            </div>
        );
    }

    if (isSingleScript) {
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

function ConnectionStatusCell({
    status,
    processNames,
    location,
    toggleErrorModal,
    hasErrors,
    nextBatchRetryTime,
    onRetrySuccess,
}: ConnectionStatusCellProps) {
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const retryBatch = useAsyncCallback(async () => {
        await Promise.all(processNames.map((name) => tasksService.retryBatch(databaseName, name, location)));
        await onRetrySuccess?.();
    });

    if (status !== "Reconnect") {
        return <span>{status}</span>;
    }

    const isRetryDisabled = retryBatch.loading || !hasDatabaseWriteAccess;

    return (
        <div className="hstack gap-1">
            {status}
            <PopoverWithHoverWrapper
                message={
                    <div className="vstack gap-2 p-1">
                        <div className="vstack">
                            <div className="hstack gap-1">
                                <Icon icon="clock" margin="m-0" />
                                Next batch retry time:{" "}
                                <b>
                                    {nextBatchRetryTime
                                        ? moment(nextBatchRetryTime).format(genUtils.dateFormat)
                                        : "N/A"}
                                </b>
                            </div>
                            {nextBatchRetryTime && (
                                <small className="text-right"> ({moment(nextBatchRetryTime).fromNow()})</small>
                            )}
                        </div>
                        <div className="d-flex gap-2">
                            <ButtonWithSpinner
                                variant="primary"
                                size="sm"
                                className="rounded-pill"
                                icon="refresh"
                                isSpinning={retryBatch.loading}
                                onClick={retryBatch.execute}
                                disabled={isRetryDisabled}
                            >
                                Retry now
                            </ButtonWithSpinner>
                            <Button
                                variant="secondary"
                                size="sm"
                                className="rounded-pill"
                                onClick={toggleErrorModal}
                                disabled={!hasErrors}
                            >
                                <Icon icon="preview" />
                                View error
                            </Button>
                        </div>
                    </div>
                }
            >
                <Icon icon="info" color="info" margin="m-0" />
            </PopoverWithHoverWrapper>
        </div>
    );
}

function ItemWithTooltip(props: ItemWithTooltipProps) {
    const { nodeInfo, sharded, task, showPreview, etlStats, txIdLayout } = props;

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

    const { open } = useViewSheet();

    const key = taskNodeInfoKey(nodeInfo);
    const hasError = !!nodeInfo.details?.error;
    const [node, setNode] = useState<HTMLDivElement>();

    const { appUrl } = useAppUrls();
    const { tasksService } = useServices();
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const databaseName = db?.name;

    const processNames = (nodeInfo.etlProgress ?? []).map(
        (progress) => `${task.shared.taskName}/${progress.transformationName}`
    );

    const asyncLocalEtlStats = useAsync(
        () => tasksService.getEtlStats(databaseName, nodeInfo.location, processNames),
        []
    );

    const asyncEtlErrors = useAsync(() => tasksService.getEtlErrors(databaseName, nodeInfo.location, processNames), []);

    const openErrorSheet = () => {
        const etlErrorsList = asyncEtlErrors.result ?? [];
        const tasksWithErrors = getTasksWithErrors(
            etlErrorsList.map((e) => ({
                ...e,
                nodeTag: nodeInfo.location.nodeTag,
                shardNumber: nodeInfo.location.shardNumber,
            })),
            etlStats
        );
        const allErrors = flattenAllTasksErrors(tasksWithErrors, etlStats ?? []);

        if (allErrors.length === 0) {
            return;
        }

        const mostRecentError = allErrors.sort(
            (a, b) => new Date(b.CreatedAt).getTime() - new Date(a.CreatedAt).getTime()
        )[0];

        open({
            component: <EtlErrorDetailsSheet error={mostRecentError} allErrors={allErrors} initialIndex={0} />,
            initialWidth: "40%",
            minWidth: "25%",
            maxWidth: "60%",
        });
    };

    const locationEtlStats = (etlStats ?? []).filter((s) =>
        databaseLocationComparator({ nodeTag: s.NodeTag, shardNumber: s.ShardNumber }, nodeInfo.location)
    );
    const taskHealth = getTaskHealthStatus(locationEtlStats, task.shared.taskName);
    const { bg, icon: heathIcon, label: healthLabel } = healthStatusToBadge(taskHealth);
    const errorCount = getTaskErrorCount(asyncEtlErrors.result ?? [], task.shared.taskName);
    const goToTaskErrors = appUrl.forTasksErrors(databaseName, { taskName: task.shared.taskName });

    const nextBatchRetryTime =
        findNextBatchRetryTime(asyncLocalEtlStats.result, task.shared.taskName) ??
        findNextBatchRetryTime(etlStats, task.shared.taskName);

    return (
        <div ref={setNode}>
            <DistributionItem loading={nodeInfo.status === "loading" || nodeInfo.status === "idle"} key={key}>
                {sharded && shard}
                <div className={classNames("node", { top: !sharded })}>
                    {!sharded && <Icon icon="node" />}
                    {nodeInfo.location.nodeTag}
                </div>
                {nodeInfo.status === "success" && (
                    <div>
                        <ConnectionStatusCell
                            status={nodeInfo.details.taskConnectionStatus}
                            processNames={processNames}
                            location={nodeInfo.location}
                            toggleErrorModal={openErrorSheet}
                            hasErrors={errorCount > 0}
                            nextBatchRetryTime={nextBatchRetryTime}
                            onRetrySuccess={asyncLocalEtlStats.execute}
                        />
                    </div>
                )}
                <div>
                    <EtlErrorCountCell
                        hasError={hasError}
                        errorCount={errorCount}
                        taskHealth={taskHealth}
                        goToTaskErrors={goToTaskErrors}
                    />
                </div>
                <div className="d-flex align-items-center">
                    <PopoverWithHoverWrapper
                        wrapperClassName="d-flex align-items-center"
                        message={getPopoverMessageForTaskHealth(taskHealth)}
                    >
                        <Badge bg={bg} className="rounded-pill">
                            <Icon icon={heathIcon} />
                            {healthLabel}
                        </Badge>
                    </PopoverWithHoverWrapper>
                </div>
                {txIdLayout && <TransactionalIdCells txIdLayout={txIdLayout} nodeInfo={nodeInfo} />}
                <OngoingEtlTaskProgress task={task} nodeInfo={nodeInfo} />
            </DistributionItem>
            {node && (
                <OngoingEtlTaskProgressTooltip
                    hasError={!!nodeInfo.details?.error}
                    toggleErrorModal={openErrorSheet}
                    target={node}
                    progress={nodeInfo.etlProgress}
                    status={nodeInfo.status}
                    showPreview={showPreview}
                />
            )}
        </div>
    );
}

interface EtlErrorCountCellProps {
    hasError: boolean;
    errorCount: number;
    taskHealth: EtlHealthStatus;
    goToTaskErrors: string;
}

function EtlErrorCountCell({ hasError, errorCount, taskHealth, goToTaskErrors }: EtlErrorCountCellProps) {
    if (hasError || errorCount > 0) {
        return (
            <strong>
                <a
                    href={goToTaskErrors}
                    className="d-flex text-decoration-none text-white align-items-center gap-1 no-decor"
                >
                    <Icon icon="warning" color="danger" margin="m-0" />
                    {errorCount > 0 && <b>{errorCount}</b>}
                </a>
            </strong>
        );
    }

    if (taskHealth === "Failed" || taskHealth === "Impaired") {
        return (
            <PopoverWithHoverWrapper
                message="No errors are currently stored. The health status reflects recent error activity and will recover automatically as new batches complete successfully."
                placement="top"
            >
                -
            </PopoverWithHoverWrapper>
        );
    }

    return "-";
}

function getTxIdLayout(task: AnyEtlOngoingTaskInfo, visibleNodes: OngoingEtlTaskNodeInfo[]): TxIdLayout | null {
    if (task.shared.taskType !== "KafkaQueueEtl") {
        return null;
    }

    const scripts = Array.from(
        visibleNodes.reduce((acc, nodeInfo) => {
            nodeInfo.etlProgress?.forEach((ep) => {
                if (ep.transactionalId) {
                    acc.add(ep.transformationName);
                }
            });
            return acc;
        }, new Set<string>())
    );

    return { scripts, isSingleScript: scripts.length === 1 };
}

export function OngoingEtlTaskDistribution(props: OngoingEtlTaskDistributionProps) {
    const { task, showPreview, etlStats } = props;
    const sharded = task.nodesInfo.some((x) => x.location.shardNumber != null);

    const visibleNodes = task.nodesInfo.filter(
        (nodeInfo) =>
            nodeInfo.details && task.responsibleLocations.find((l) => databaseLocationComparator(l, nodeInfo.location))
    );

    const txIdLayout = getTxIdLayout(task, visibleNodes);

    const items = visibleNodes.map((nodeInfo) => {
        const key = taskNodeInfoKey(nodeInfo);

        return (
            <ItemWithTooltip
                key={key}
                nodeInfo={nodeInfo}
                sharded={sharded}
                showPreview={showPreview}
                task={task}
                etlStats={etlStats}
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
                        <Icon icon="connected" /> Connection status
                    </div>
                    <div>
                        <Icon icon="warning" /> Errors
                    </div>
                    <div>
                        <Icon icon="healthcheck" /> Health status
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

function findNextBatchRetryTime(stats: EtlTaskStats[] | undefined, taskName: string): string | undefined {
    return stats
        ?.find((s: EtlTaskStats) => s.TaskName === taskName)
        ?.Stats?.find((s: EtlTaskStats["Stats"][number]) => s.Statistics.NextBatchRetryTime != null)?.Statistics
        .NextBatchRetryTime;
}
