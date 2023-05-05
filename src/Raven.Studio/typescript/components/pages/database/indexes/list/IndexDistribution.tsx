import { IndexNodeInfo, IndexSharedInfo } from "components/models/indexes";
import React, { useState } from "react";
import classNames from "classnames";
import { IndexProgressTooltip } from "./IndexProgressTooltip";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import IndexUtils from "../../../../utils/IndexUtils";
import {
    DistributionItem,
    DistributionLegend,
    DistributionSummary,
    LocationDistribution,
} from "components/common/LocationDistribution";
import assertUnreachable from "../../../../utils/assertUnreachable";
import { ProgressCircle } from "components/common/ProgressCircle";
import { Button } from "reactstrap";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";

interface IndexDistributionProps {
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
    showStaleReason: (location: databaseLocationSpecifier) => void;
    openFaulty: (location: databaseLocationSpecifier) => void;
}

interface ItemWithTooltipProps {
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
    showStaleReason: (location: databaseLocationSpecifier) => void;
    openFaulty: (location: databaseLocationSpecifier) => void;
    nodeInfo: IndexNodeInfo;
    sharded: boolean;
}

function ItemWithTooltip(props: ItemWithTooltipProps) {
    const { nodeInfo, sharded, openFaulty, showStaleReason, globalIndexingStatus, index } = props;
    const entriesCount = nodeInfo.details?.faulty ? "n/a" : nodeInfo.details?.entriesCount ?? "";

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

    const [node, setNode] = useState<HTMLDivElement>();

    return (
        <div ref={setNode}>
            <DistributionItem loading={nodeInfo.status === "loading" || nodeInfo.status === "idle"}>
                {sharded && shard}
                <div className={classNames("node", { top: !sharded })}>
                    {!sharded && <Icon icon="node" />}

                    {nodeInfo.location.nodeTag}
                </div>
                <div className="entries">{entriesCount.toLocaleString()}</div>
                <div className="errors">{nodeInfo.details?.errorCount.toLocaleString() ?? ""}</div>

                <IndexProgress nodeInfo={nodeInfo} />

                {nodeInfo.details?.faulty && (
                    <div className="text-center">
                        <Button
                            color="danger"
                            className="px-1 py-0 my-1"
                            size="xs"
                            onClick={() => openFaulty(nodeInfo.location)}
                        >
                            Open faulty index
                        </Button>
                    </div>
                )}
            </DistributionItem>
            {node && (
                <IndexProgressTooltip
                    target={node}
                    nodeInfo={nodeInfo}
                    index={index}
                    globalIndexingStatus={globalIndexingStatus}
                    showStaleReason={showStaleReason}
                />
            )}
        </div>
    );
}

export function IndexDistribution(props: IndexDistributionProps) {
    const { index, globalIndexingStatus, showStaleReason, openFaulty } = props;

    const totalErrors = index.nodesInfo
        .filter((x) => x.status === "success")
        .reduce((prev, current) => prev + current.details.errorCount, 0);
    const estimatedEntries = IndexUtils.estimateEntriesCount(index)?.toLocaleString() ?? "-";

    const sharded = IndexUtils.isSharded(index);

    const items = (
        <>
            {index.nodesInfo.map((nodeInfo) => {
                const key = indexNodeInfoKey(nodeInfo);
                return (
                    <ItemWithTooltip
                        key={key}
                        nodeInfo={nodeInfo}
                        sharded={sharded}
                        index={index}
                        globalIndexingStatus={globalIndexingStatus}
                        showStaleReason={showStaleReason}
                        openFaulty={openFaulty}
                    />
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
                        <Icon icon="node" /> Node
                    </div>
                )}
                <div>
                    <Icon icon="list" /> Entries
                </div>
                <div>
                    <Icon icon="warning" /> Errors
                </div>
                <div>
                    <Icon icon="changes" />
                    Status
                </div>
            </DistributionLegend>
            <DistributionSummary>
                <div className="top">Total</div>
                {sharded && <div> </div>}
                <div>{estimatedEntries}</div>
                <div>{totalErrors}</div>
                <div></div>
            </DistributionSummary>
            {items}
        </LocationDistribution>
    );
}

interface IndexProgressProps {
    nodeInfo: IndexNodeInfo;
}

function iconForState(status: Raven.Client.Documents.Indexes.IndexRunningStatus): IconName {
    switch (status) {
        case "Disabled":
            return "stop";
        case "Paused":
            return "pause";
        case "Pending":
            return "waiting";
        case "Running":
            return "check";
        default:
            assertUnreachable(status);
    }
}

interface JoinedIndexProgressProps {
    index: IndexSharedInfo;
    onClick: () => void;
}

function calculateOverallProgress(index: IndexSharedInfo) {
    const allProgresses = index.nodesInfo.filter((x) => x.status === "success" && x.progress);
    if (!allProgresses.length) {
        return 0;
    }

    const processed = allProgresses.reduce((p, c) => p + c.progress.global.processed, 0);
    const total = allProgresses.reduce((p, c) => p + c.progress.global.total, 0);

    if (total === 0) {
        return 1;
    }

    return processed / total;
}

function getState(index: IndexSharedInfo) {
    if (index.nodesInfo.some((x) => x.status === "failure" || x.details?.faulty || x.details?.state === "Error")) {
        return "failed";
    }
    if (
        index.nodesInfo.some(
            (x) =>
                x.details?.status === "Disabled" ||
                x.details?.status === "Paused" ||
                x.details?.status === "Pending" ||
                x.details?.stale
        )
    ) {
        return "running";
    }
    return "success";
}

function getIcon(index: IndexSharedInfo): IconName {
    if (index.nodesInfo.some((x) => x.status === "failure" || x.details?.faulty || x.details.state === "Error")) {
        return "cancel";
    }
    if (index.nodesInfo.some((x) => x.details?.status === "Disabled")) {
        return iconForState("Disabled");
    }
    if (index.nodesInfo.some((x) => x.details?.status === "Paused")) {
        return iconForState("Paused");
    }
    if (index.nodesInfo.some((x) => x.details?.status === "Pending")) {
        return iconForState("Pending");
    }
    if (index.nodesInfo.some((x) => x.details?.stale)) {
        return null;
    }
    return "check";
}

function getStateText(index: IndexSharedInfo): string {
    if (index.nodesInfo.some((x) => x.status === "failure")) {
        return "Load errors";
    }
    if (index.nodesInfo.every((x) => x.details?.faulty)) {
        return "Faulty";
    }
    if (index.nodesInfo.some((x) => x.details?.faulty)) {
        return "Some faulty";
    }
    if (index.nodesInfo.every((x) => x.details?.state === "Error")) {
        return "Errors";
    }
    if (index.nodesInfo.some((x) => x.details?.state === "Error")) {
        return "Some errored";
    }
    if (index.nodesInfo.every((x) => x.details?.status === "Disabled")) {
        return "Disabled";
    }
    if (index.nodesInfo.some((x) => x.details?.status === "Disabled")) {
        return "Some disabled";
    }
    if (index.nodesInfo.every((x) => x.details?.status === "Paused")) {
        return "Paused";
    }
    if (index.nodesInfo.some((x) => x.details?.status === "Paused")) {
        return "Some paused";
    }
    if (index.nodesInfo.every((x) => x.details?.status === "Pending")) {
        return "Pending";
    }
    if (index.nodesInfo.some((x) => x.details?.status === "Pending")) {
        return "Some pending";
    }
    if (index.nodesInfo.some((x) => x.details?.stale)) {
        return "Running";
    }
    return "Up to date";
}

export function JoinedIndexProgress(props: JoinedIndexProgressProps) {
    const { index, onClick } = props;
    const overallProgress = calculateOverallProgress(index);

    return (
        <ProgressCircle
            inline
            state={getState(index)}
            icon={getIcon(index)}
            progress={overallProgress !== 0 ? overallProgress : null}
            onClick={onClick}
        >
            {getStateText(index)}
        </ProgressCircle>
    );
}

export function IndexProgress(props: IndexProgressProps) {
    const { nodeInfo } = props;

    if (nodeInfo.status === "failure") {
        return (
            <ProgressCircle state="failed" icon="cancel">
                Load error
            </ProgressCircle>
        );
    }

    if (!nodeInfo.details) {
        return null;
    }

    if (nodeInfo.details.faulty) {
        return (
            <ProgressCircle state="failed" icon="cancel">
                Faulty
            </ProgressCircle>
        );
    }

    if (nodeInfo.details.state === "Error") {
        return (
            <ProgressCircle state="failed" icon="cancel">
                Error
            </ProgressCircle>
        );
    }

    const icon = iconForState(nodeInfo.details.status);

    if (nodeInfo.details.stale) {
        const progress = nodeInfo.progress
            ? nodeInfo.progress.global.total
                ? nodeInfo.progress.global.processed / nodeInfo.progress.global.total
                : 1
            : null;

        return (
            <ProgressCircle
                state="running"
                icon={nodeInfo.details.status === "Running" ? null : icon}
                progress={progress}
            >
                {nodeInfo.details.status}
            </ProgressCircle>
        );
    }

    if (
        nodeInfo.details.status === "Paused" ||
        nodeInfo.details.status === "Disabled" ||
        nodeInfo.details.status === "Pending"
    ) {
        return (
            <ProgressCircle state="running" icon={icon}>
                {nodeInfo.details.status}
            </ProgressCircle>
        );
    }

    return (
        <ProgressCircle state="success" icon={icon}>
            up to date
        </ProgressCircle>
    );
}

const indexNodeInfoKey = (nodeInfo: IndexNodeInfo) => nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
