import { IndexNodeInfo, IndexSharedInfo, Progress } from "../../../../models/indexes";
import React, { useState } from "react";
import classNames from "classnames";
import { IndexProgressTooltip } from "./IndexProgressTooltip";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;

interface IndexDistributionProps {
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
}

export function IndexDistribution(props: IndexDistributionProps) {
    const { index, globalIndexingStatus } = props;

    const [indexId] = useState(() => _.uniqueId("index-id"));

    return (
        <div className="index-distribution">
            <div className="distribution-legend">
                <div className="top"></div>
                <div className="node">
                    <i className="icon-node" /> Node
                </div>
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
            {/*TODO <div className="distribution-summary">
                <div className="top">
                    <i className="icon-sum" />
                </div>
                <div> </div>
                <div> TODO </div>
                <div> TODO </div>
                <div></div>
            </div>*/}

            {index.nodesInfo.map((nodeInfo) => {
                const shard = (
                    <div className="top shard">
                        {nodeInfo.location.shardNumber != null && (
                            <>
                                <i className="icon-shard" />
                                Shard #{nodeInfo.location.shardNumber}
                            </>
                        )}
                    </div>
                );

                const key = indexNodeInfoKey(nodeInfo);
                const id = indexId + key;

                return (
                    <div
                        id={id}
                        className={classNames("distribution-item", { loading: nodeInfo.status === "loading" })}
                        key={key}
                    >
                        {shard}
                        <div className="node">{nodeInfo.location.nodeTag}</div>
                        <div className="entries">{nodeInfo.details?.entriesCount ?? ""}</div>
                        <div className="errors">{nodeInfo.details?.errorCount ?? ""}</div>
                        <IndexState nodeInfo={nodeInfo} />

                        <IndexProgressTooltip
                            target={id}
                            nodeInfo={nodeInfo}
                            index={index}
                            globalIndexingStatus={globalIndexingStatus}
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

function IndexState(props: IndexStateProps) {
    const { nodeInfo } = props;
    if (!nodeInfo.details) {
        return null;
    }

    if (nodeInfo.details.status === "Disabled") {
        return (
            <div className="state pending">
                <div className="state-desc">disabled</div>
                <div className="state-indicator">
                    <i className="icon-cancel" />
                </div>
            </div>
        );
    }

    if (nodeInfo.details.status === "Paused") {
        return (
            <div className="state pending">
                <div className="state-desc">paused</div>
                <div className="state-indicator">
                    <i className="icon-pause" />
                </div>
            </div>
        );
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
                        <strong>{(100 * progress).toFixed(0)}%</strong> running
                    </div>
                    <div className="state-indicator">
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

function isCompleted(progress: Progress, stale: boolean) {
    return progress.processed === progress.total && !stale;
}
