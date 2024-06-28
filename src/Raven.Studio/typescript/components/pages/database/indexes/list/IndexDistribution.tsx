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
import IndexDistributionStatusChecker from "./IndexDistributionStatusChecker";
import moment = require("moment");
import genUtils = require("common/generalUtils");

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

function getFormattedTime(date: Date): string {
    return date ? genUtils.formatDurationByDate(moment(date)) + " ago" : "-";
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
                <div className="text-center">{getFormattedTime(nodeInfo.details?.lastIndexingTime)}</div>
                <div className="text-center">{getFormattedTime(nodeInfo.details?.lastQueryingTime)}</div>

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

    const earliestCreatedTimestamp = IndexUtils.getEarliestCreatedTimestamp(index);
    const formattedEarliestCreatedTimestamp = earliestCreatedTimestamp
        ? genUtils.formatDurationByDate(moment(earliestCreatedTimestamp)) + " ago"
        : null;

    const items = (
        <>
            {[...index.nodesInfo]
                .sort((l, r) => l.location.shardNumber - r.location.shardNumber)
                .map((nodeInfo) => {
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
        <div>
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
                        <Icon icon="index-history" /> Indexed
                    </div>
                    <div>
                        <Icon icon="queries" /> Queried
                    </div>
                    <div>
                        <Icon icon="changes" /> State
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
            {formattedEarliestCreatedTimestamp && (
                <div className="small">
                    <span className="text-muted">Created:</span> <strong>{formattedEarliestCreatedTimestamp}</strong>
                </div>
            )}
        </div>
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

function getState(statusChecker: IndexDistributionStatusChecker) {
    if (statusChecker.everyFailure() || statusChecker.someFaulty() || statusChecker.someErrors()) {
        return "failed";
    }
    if (statusChecker.someFailure()) {
        return "warning";
    }
    if (
        statusChecker.someDisabled() ||
        statusChecker.somePaused() ||
        statusChecker.somePending() ||
        statusChecker.someStale()
    ) {
        return "running";
    }
    return "success";
}

function getIcon(statusChecker: IndexDistributionStatusChecker): IconName {
    if (statusChecker.everyFailure() || statusChecker.someFaulty() || statusChecker.someErrors()) {
        return "cancel";
    }
    if (statusChecker.someFailure()) {
        return "warning";
    }
    if (statusChecker.someDisabled()) {
        return iconForState("Disabled");
    }
    if (statusChecker.somePaused()) {
        return iconForState("Paused");
    }
    if (statusChecker.somePending()) {
        return iconForState("Pending");
    }
    if (statusChecker.someStale()) {
        return null;
    }
    return "check";
}

function getStateText(statusChecker: IndexDistributionStatusChecker): string {
    if (statusChecker.everyFailure()) {
        return "Load errors";
    }
    if (statusChecker.someFailure()) {
        return "Some load errors";
    }
    if (statusChecker.everyFaulty()) {
        return "Faulty";
    }
    if (statusChecker.someFaulty()) {
        return "Some faulty";
    }
    if (statusChecker.everyErrors()) {
        return "Errors";
    }
    if (statusChecker.someErrors()) {
        return "Some errored";
    }
    if (statusChecker.everyDisabled()) {
        return "Disabled";
    }
    if (statusChecker.someDisabled()) {
        return "Some disabled";
    }
    if (statusChecker.everyPaused()) {
        return "Paused";
    }
    if (statusChecker.somePaused()) {
        return "Some paused";
    }
    if (statusChecker.everyPending()) {
        return "Pending";
    }
    if (statusChecker.somePending()) {
        return "Some pending";
    }
    if (statusChecker.someStale()) {
        return "Running";
    }
    return "Up to date";
}

export function JoinedIndexProgress(props: JoinedIndexProgressProps) {
    const { index, onClick } = props;

    const overallProgress = calculateOverallProgress(index);

    const statusChecker = new IndexDistributionStatusChecker(index);
    const stateText = getStateText(statusChecker);

    return (
        <ProgressCircle
            inline
            state={getState(statusChecker)}
            icon={getIcon(statusChecker)}
            progress={stateText === "Running" ? overallProgress : null}
            onClick={onClick}
        >
            {stateText}
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
