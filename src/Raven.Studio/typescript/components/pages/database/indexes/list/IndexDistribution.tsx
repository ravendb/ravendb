import { IndexNodeInfo, IndexSharedInfo } from "../../../../models/indexes";
import React from "react";


interface IndexDistributionProps {
    index: IndexSharedInfo;
}

export function IndexDistribution(props: IndexDistributionProps) {
    const { index } = props;
    return (
        <div className="index-distribution">
            <div className="distribution-legend">
                <div className="top"></div>
                <div className="node"><i className="icon-node" /> Node </div>
                <div> <i className="icon-list" /> Entries </div>
                <div> <i className="icon-warning" /> Errors </div>
                <div> <i />Status </div>
            </div>
            <div className="distribution-summary">
                <div className="top">
                    <i className="icon-sum" />
                </div>
                <div> </div>
                <div> 4 802 809 </div>
                <div> 0 </div>
                <div></div>
            </div>

            {index.nodesInfo.map(nodeInfo => {
                const shard = <div className="top shard">
                    {nodeInfo.location.shardNumber != null && (
                        <>
                            <i className="icon-shard"/>
                            Shard #{nodeInfo.location.shardNumber}
                        </>
                    )}
                </div>;
                
                if (nodeInfo.status === "loaded") {
                    return (
                        <div className="distribution-item" key={indexNodeInfoKey(nodeInfo)}>
                            {shard}
                            <div className="node">{nodeInfo.location.nodeTag}</div>
                            <div className="entries">{nodeInfo.details.entriesCount}</div>
                            <div className="errors">{nodeInfo.details.errorCount}</div>
                            <div className="state up-to-date">
                                <div className="state-desc">up to date</div>
                                <div className="state-indicator"><i className="icon-check" /></div>
                            </div>
                        </div>
                    )
                }
                return (
                    <div className="distribution-item loading" key={indexNodeInfoKey(nodeInfo)}>
                        {shard}
                        <div className="node">{nodeInfo.location.nodeTag}</div>
                        <div className="entries"></div>
                        <div className="errors"></div>
                        <div className="state"></div>
                    </div>
                )
            })}
        </div>
    )
}


const indexNodeInfoKey = (nodeInfo: IndexNodeInfo) =>
    "$" + nodeInfo.location.shardNumber + "@" + nodeInfo.location.nodeTag;
