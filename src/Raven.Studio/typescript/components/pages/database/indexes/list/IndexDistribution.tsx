import { IndexNodeInfo, IndexSharedInfo } from "../../../../models/indexes";
import React, { useState } from "react";
import classNames from "classnames";
import { IndexProgressTooltip } from "./IndexProgressTooltip";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import IndexUtils from "../../../../utils/IndexUtils";

interface IndexDistributionProps {
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
    showStaleReason: (location: databaseLocationSpecifier) => void;
}

export function IndexDistribution(props: IndexDistributionProps) {
    const { index, globalIndexingStatus, showStaleReason } = props;

    const [indexId] = useState(() => _.uniqueId("index-id"));

    const totalErrors = index.nodesInfo
        .filter((x) => x.status === "loaded")
        .reduce((prev, current) => prev + current.details.errorCount, 0);
    const estimatedEntries = IndexUtils.estimateEntriesCount(index);

    const sharded = IndexUtils.isSharded(index);

    return (
        <div className="index-distribution">
            <div className="distribution-legend">
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
            </div>
            {index.nodesInfo.length > 1 && (
                <div className="distribution-summary">
                    <div className="top">Total</div>
                    {sharded && <div> </div>}
                    <div>
                        {estimatedEntries.estimated && estimatedEntries.entries != null ? "~" : ""}
                        {estimatedEntries.entries?.toLocaleString() ?? "-"}
                    </div>
                    <div>{totalErrors}</div>
                    <div></div>
                </div>
            )}

            {index.nodesInfo.map((nodeInfo) => {
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

                const key = indexNodeInfoKey(nodeInfo);
                const id = indexId + key;

                return (
                    <div
                        id={id}
                        className={classNames("distribution-item", {
                            loading: nodeInfo.status === "loading" || nodeInfo.status === "notLoaded",
                        })}
                        key={key}
                    >
                        {sharded && shard}
                        <div className={classNames("node", { top: !sharded })}>
                            {!sharded && <i className="icon-node"></i>}

                            {nodeInfo.location.nodeTag}
                        </div>
                        <div className="entries">{nodeInfo.details?.entriesCount ?? ""}</div>
                        <div className="errors">{nodeInfo.details?.errorCount ?? ""}</div>
                        <IndexState nodeInfo={nodeInfo} />

                        <IndexProgressTooltip
                            target={id}
                            nodeInfo={nodeInfo}
                            index={index}
                            globalIndexingStatus={globalIndexingStatus}
                            showStaleReason={showStaleReason}
                        />
                    </div>
                );
            })}
        </div>
    );
}

interface IndexStateProps {
    nodeInfo: IndexNodeInfo;
}

const stateIndicatorProgressRadius = 13;

export function IndexState(props: IndexStateProps) {
    const { nodeInfo } = props;
    if (!nodeInfo.details) {
        return null;
    }

    if (nodeInfo.details.state === "Error") {
        return (
            <div className="state failed">
                <div className="state-desc">error</div>
                <div className="state-indicator">
                    <i className="icon-cancel" />
                </div>
            </div>
        );
    }

    if (nodeInfo.progress) {
        const circumference = 2 * Math.PI * stateIndicatorProgressRadius;
        const progress = nodeInfo.progress.global.total
            ? nodeInfo.progress.global.processed / nodeInfo.progress.global.total
            : 1;

        if (nodeInfo.details.stale) {
            return (
                <div className="state running">
                    <div className="state-desc">
                        <strong>{(100 * progress).toFixed(0)}%</strong>
                        {nodeInfo.details.status === "Paused" && <>Paused</>}
                        {nodeInfo.details.status === "Disabled" && <>Disabled</>}
                        {nodeInfo.details.status === "Running" && <>Running</>}
                        {nodeInfo.details.status === "Pending" && <>Pending</>}
                    </div>
                    <div className="state-indicator">
                        {nodeInfo.details.status === "Paused" && <i className="icon-pause" />}
                        {nodeInfo.details.status === "Disabled" && <i className="icon-stop" />}
                        <svg className="progress-ring">
                            <circle strokeDashoffset={circumference * (1.0 - progress)} />
                        </svg>
                    </div>
                </div>
            );
        }
        /* TODO pending
        <div className="state pending">
            <div className='state-desc'>pending</div>
            <div className="state-indicator"><i className="icon-waiting" /></div>
        </div>
         */
    }

    if (nodeInfo.details.status === "Paused") {
        return (
            <div className="state running">
                <div className="state-desc">Paused</div>
                <div className="state-indicator">
                    <i className="icon-pause" />
                </div>
            </div>
        );
    }
    if (nodeInfo.details.status === "Disabled") {
        return (
            <div className="state running">
                <div className="state-desc">Disabled</div>
                <div className="state-indicator">
                    <i className="icon-stop" />
                </div>
            </div>
        );
    }

    return (
        <div className="state up-to-date">
            <div className="state-desc">up to date</div>
            <div className="state-indicator">
                <i className="icon-check" />
            </div>
        </div>
    );
}

const indexNodeInfoKey = (nodeInfo: IndexNodeInfo) => nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
