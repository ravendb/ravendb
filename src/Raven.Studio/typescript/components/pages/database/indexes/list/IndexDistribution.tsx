import { IndexNodeInfo, IndexSharedInfo } from "components/models/indexes";
import React, { useRef, useState } from "react";
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
                    <i className="icon-shard" />
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
                    {!sharded && <i className="icon-node"></i>}

                    {nodeInfo.location.nodeTag}
                </div>
                <div className="entries">{entriesCount}</div>
                <div className="errors">{nodeInfo.details?.errorCount ?? ""}</div>

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
                        <i className="icon-node" /> Node
                    </div>
                )}
                <div>
                    <i className="icon-list" /> Entries
                </div>
                <div>
                    <i className="icon-warning" /> Errors
                </div>
                <div>
                    <i />
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

function iconForState(status: Raven.Client.Documents.Indexes.IndexRunningStatus) {
    switch (status) {
        case "Disabled":
            return "icon-stop";
        case "Paused":
            return "icon-pause";
        case "Pending":
            return "icon-waiting";
        case "Running":
            return "icon-check";
        default:
            assertUnreachable(status);
    }
}

interface JoinedIndexProgressProps {
    index: IndexSharedInfo;
}

export function JoinedIndexProgress(props: JoinedIndexProgressProps) {
    const { index } = props;

    if (index.nodesInfo.some((x) => x.status === "failure")) {
        return (
            <ProgressCircle inline state="failed" icon="icon-cancel">
                Load error
            </ProgressCircle>
        );
    }
    if (index.nodesInfo.some((x) => x.details?.faulty)) {
        return (
            <ProgressCircle inline state="failed" icon="icon-cancel">
                Faulty
            </ProgressCircle>
        );
    }

    if (index.nodesInfo.some((x) => x.details?.state === "Error")) {
        return (
            <ProgressCircle inline state="failed" icon="icon-cancel">
                Error
            </ProgressCircle>
        );
    }

    if (index.nodesInfo.some((x) => x.details?.status === "Disabled")) {
        return (
            <ProgressCircle inline state="running" icon={iconForState("Disabled")}>
                Disabled
            </ProgressCircle>
        );
    }

    if (index.nodesInfo.some((x) => x.details?.status === "Paused")) {
        return (
            <ProgressCircle inline state="running" icon={iconForState("Paused")}>
                Paused
            </ProgressCircle>
        );
    }

    if (index.nodesInfo.some((x) => x.details?.status === "Pending")) {
        return (
            <ProgressCircle inline state="running" icon={iconForState("Pending")}>
                Pending
            </ProgressCircle>
        );
    }

    if (index.nodesInfo.some((x) => x.progress)) {
        return (
            <ProgressCircle inline state="running">
                Running
            </ProgressCircle>
        );
    }

    return (
        <ProgressCircle inline state="success" icon="icon-check">
            up to date
        </ProgressCircle>
    );
}

export function IndexProgress(props: IndexProgressProps) {
    const { nodeInfo } = props;

    if (nodeInfo.status === "failure") {
        return (
            <ProgressCircle state="failed" icon="icon-cancel">
                Load error
            </ProgressCircle>
        );
    }

    if (!nodeInfo.details) {
        return null;
    }

    if (nodeInfo.details.faulty) {
        return (
            <ProgressCircle state="failed" icon="icon-cancel">
                Faulty
            </ProgressCircle>
        );
    }

    if (nodeInfo.details.state === "Error") {
        return (
            <ProgressCircle state="failed" icon="icon-cancel">
                Error
            </ProgressCircle>
        );
    }

    const icon = iconForState(nodeInfo.details.status);

    if (nodeInfo.progress) {
        const progress = nodeInfo.progress.global.total
            ? nodeInfo.progress.global.processed / nodeInfo.progress.global.total
            : 1;

        if (nodeInfo.details.stale) {
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
