import React from "react";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import "./NodeLocationSelect.scss";

interface NodeLocationSelectProps {
    locations: { nodeTag: string; shardNumber?: number }[];
    selectedIndex: number;
    onChange: (index: number) => void;
    className?: string;
}

export function NodeLocationTabs({ locations, selectedIndex, onChange, className }: NodeLocationSelectProps) {
    return (
        <div className={classNames("node-location-tabs", className)}>
            {locations.map((loc, i) => {
                const isActive = i === selectedIndex;
                return (
                    <button
                        key={i}
                        type="button"
                        className={classNames("node-location-tab", { active: isActive })}
                        onClick={() => onChange(i)}
                    >
                        <Icon icon="node" color="node" margin="m-0" />
                        <span>{loc.nodeTag}</span>
                        {loc.shardNumber != null && (
                            <>
                                <Icon icon="shard" color="shard" margin="m-0" />
                                <span>#{loc.shardNumber}</span>
                            </>
                        )}
                    </button>
                );
            })}
        </div>
    );
}
