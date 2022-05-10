const shardImg = require("Content/img/sharding/shard.svg");
const nodeImg = require("Content/img/node.svg");

import React from "react";

interface IndexProgressTooltipProps {
    
}

export function IndexProgressTooltip(props: IndexProgressTooltipProps) {
    
    //TODO: create hook for tooltip with hover
    
    return (
        <div className="index-tooltip">
            <div className="index-location">

                <div className="shard-id">
                    <div className="small-label">SHARD</div>
                    <img src={shardImg} /> 1
                </div>

                <div className="node-id">
                    <div className="small-label">NODE</div>
                    <img src={nodeImg} /> A
                </div>

            </div>
            <div className="index-details">
                <div className="details-item state"><div className="state-pill">Normal</div></div>
                <div className="details-item entries"><i className="icon-list" /> 4200 entries</div>
                <div className="details-item errors text-danger"><i className="icon-warning" /> 2 errors</div>
                <div className="details-item status updating"><i className="icon-waiting" /> Updating</div>
            </div>

            <div className="collection-name">Orders</div>
            <div className="collection-progress">
                <div className="progress-item">
                    <strong className="progress-percentage">50%</strong> <span>documents</span>
                    <div className="progress">
                        <div className="progress-bar" style={{ width: "50%" }} />
                    </div>
                </div>
                <div className="progress-item">
                    <strong className="progress-percentage">40%</strong> <span>tombstones</span>
                    <div className="progress">
                        <div className="progress-bar" style={{ width: "40%" }} />
                    </div>
                </div>
            </div>

            <div className="collection-name">Users</div>
            <div className="collection-progress">
                <div className="progress-item">
                    <strong className="progress-percentage">24%</strong> <span>documents</span>
                    <div className="progress">
                        <div className="progress-bar" style={{width: "24%"}} />
                    </div>
                </div>
                <div className="progress-item">
                    <strong className="progress-percentage">30%</strong> <span>tombstones</span>
                    <div className="progress">
                        <div className="progress-bar" style={{ width: "30%" }} />
                    </div>
                </div>
            </div>
        </div>
    )
}
