import React from "react";
import "./UsedIdsPill.scss";
import { Icon } from "components/common/Icon";
import copyToClipboard from "common/copyToClipboard";

interface UsedIdsPillProps {
    vector: string;
    node: string;
    shard: string;
}
export default function UsedIdsPill(props: UsedIdsPillProps) {
    const { vector, node, shard } = props;
    return (
        <div className="used-id-pill">
            <strong
                className="vector-pill text-truncate"
                title={vector}
                onClick={() => copyToClipboard.copy(vector, `Copied ${vector} vector to clipboard`)}
            >
                {vector}
            </strong>
            <div className="d-flex gap-1 ms-auto">
                <span className="text-muted" title={`Node ${node}`}>
                    <Icon icon="node" color="node" />
                    {node}
                </span>
                <span className="text-muted" title={`Shard ${shard}`}>
                    <Icon icon="shard" color="shard" />
                    {shard}
                </span>
            </div>
        </div>
    );
}
