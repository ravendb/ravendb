import React from "react";
import { ReactNode } from "react";
import classNames from "classnames";

import "./LocationSpecificDetails.scss";

const shardImg = require("Content/img/sharding/shard.svg");
const nodeImg = require("Content/img/node.svg");

export function LocationSpecificDetails(props: {
    children: ReactNode | ReactNode[];
    location: databaseLocationSpecifier;
}) {
    const { children, location } = props;
    return (
        <div className="location-specific-details-tooltip">
            <div className="location-specific-details-location">
                {location.shardNumber != null && (
                    <div className="shard-id">
                        <div className="small-label">SHARD</div>
                        <img src={shardImg} alt="Shard Number" /> {location.shardNumber}
                    </div>
                )}
                <div className="node-id">
                    <div className="small-label">NODE</div>
                    <img src={nodeImg} alt="Node Tag" /> {location.nodeTag}
                </div>
            </div>
            {children}
        </div>
    );
}

export function LocationSpecificDetailsItemsContainer(props: { children: ReactNode | ReactNode[] }) {
    return <div className="details">{props.children}</div>;
}

export function LocationSpecificDetailsItem(props: { children: ReactNode | ReactNode[]; className?: string }) {
    const { children, className } = props;
    return <div className={classNames("details-item", className)}>{children}</div>;
}
