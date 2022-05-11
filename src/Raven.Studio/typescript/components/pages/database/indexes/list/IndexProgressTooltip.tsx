import { IndexNodeInfo, IndexNodeInfoDetails, IndexSharedInfo, Progress } from "../../../../models/indexes";

const shardImg = require("Content/img/sharding/shard.svg");
const nodeImg = require("Content/img/node.svg");

import React from "react";
import { PopoverWithHover } from "../../../../common/PopoverWithHover";
import classNames from "classnames";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import IndexUtils from "../../../../utils/IndexUtils";

interface IndexProgressTooltipProps {
    target: string;
    nodeInfo: IndexNodeInfo;
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
}

export function IndexProgressTooltip(props: IndexProgressTooltipProps) {
    const { target, nodeInfo, index, globalIndexingStatus } = props;

    if (!nodeInfo.details) {
        return null;
    }

    return (
        <PopoverWithHover target={target} placement="right" delay={100}>
            <div className="index-tooltip">
                <div className="index-location">
                    {nodeInfo.location.shardNumber != null && (
                        <div className="shard-id">
                            <div className="small-label">SHARD</div>
                            <img src={shardImg} alt="Shard Number" /> {nodeInfo.location.shardNumber}
                        </div>
                    )}
                    <div className="node-id">
                        <div className="small-label">NODE</div>
                        <img src={nodeImg} alt="Node Tag" /> {nodeInfo.location.nodeTag}
                    </div>
                </div>
                <div className="index-details">
                    <div className="details-item state">
                        <div className="state-pill">{badgeText(index, nodeInfo.details, globalIndexingStatus)}</div>
                        {/* TODO: badgeClass */}
                    </div>
                    <div className="details-item entries">
                        <i className="icon-list" /> {nodeInfo.details.entriesCount} entries
                    </div>
                    <div
                        className={classNames("details-item errors", {
                            "text-danger": nodeInfo.details.errorCount > 0,
                        })}
                    >
                        <i className="icon-warning" /> {nodeInfo.details.errorCount} errors
                    </div>
                    {nodeInfo.details.stale ? (
                        <div className="details-item status updating">
                            <i className="icon-waiting" /> Updating
                        </div>
                    ) : (
                        <div className="details-item status">
                            <i className="icon-check" /> Up to date
                        </div>
                    )}
                </div>

                {nodeInfo.progress &&
                    nodeInfo.progress.collections.map((collection) => {
                        const docsPercentage = progressPercentage(collection.documents, false);
                        const docsPercentageFormatted = formatPercentage(docsPercentage);

                        const tombstonesPercentage = progressPercentage(collection.tombstones, false);
                        const tombstonesPercentageFormatted = formatPercentage(tombstonesPercentage);
                        return (
                            <React.Fragment key={collection.name}>
                                <div className="collection-name">{collection.name}</div>
                                <div className="collection-progress">
                                    <div className="progress-item">
                                        <strong className="progress-percentage">{docsPercentageFormatted}</strong>{" "}
                                        <span>documents</span>
                                        <div className="progress">
                                            <div className="progress-bar" style={{ width: docsPercentage + "%" }} />
                                        </div>
                                    </div>
                                    <div className="progress-item">
                                        <strong className="progress-percentage">{tombstonesPercentageFormatted}</strong>{" "}
                                        <span>tombstones</span>
                                        <div className="progress">
                                            <div
                                                className="progress-bar"
                                                style={{ width: tombstonesPercentage + "%" }}
                                            />
                                        </div>
                                    </div>
                                </div>
                            </React.Fragment>
                        );
                    })}
            </div>
        </PopoverWithHover>
    );
}

function progressPercentage(progress: Progress, stale: boolean) {
    const processed = progress.processed;
    const total = progress.total;
    if (total === 0) {
        return stale ? 99.9 : 100;
    }

    const result = Math.floor((processed * 100.0) / total);

    return result === 100 && stale ? 99.9 : result;
}

function formatPercentage(input: number) {
    const num = Math.floor(input * 10) / 10;
    return num.toString() + "%";
}

function badgeClass(index: IndexSharedInfo, details: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
    if (IndexUtils.isFaulty(index)) {
        return "badge-danger";
    }

    if (IndexUtils.isErrorState(details)) {
        return "badge-danger";
    }

    if (IndexUtils.isPausedState(details, globalIndexingStatus)) {
        return "badge-warnwing";
    }

    if (IndexUtils.isDisabledState(details, globalIndexingStatus)) {
        return "badge-warning";
    }

    if (IndexUtils.isIdleState(details, globalIndexingStatus)) {
        return "badge-warning";
    }

    if (IndexUtils.isErrorState(details)) {
        return "badge-danger";
    }

    return "badge-success";
}

function badgeText(index: IndexSharedInfo, details: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
    if (IndexUtils.isFaulty(index)) {
        return "Faulty";
    }

    if (IndexUtils.isErrorState(details)) {
        return "Error";
    }

    if (IndexUtils.isPausedState(details, globalIndexingStatus)) {
        return "Paused";
    }

    if (IndexUtils.isDisabledState(details, globalIndexingStatus)) {
        return "Disabled";
    }

    if (IndexUtils.isIdleState(details, globalIndexingStatus)) {
        return "Idle";
    }

    return "Normal";
}
