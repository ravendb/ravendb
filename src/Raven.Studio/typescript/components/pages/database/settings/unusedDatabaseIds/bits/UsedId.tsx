import React from "react";
import { Icon } from "components/common/Icon";
import copyToClipboard from "common/copyToClipboard";
import { Button } from "reactstrap";
import { UsedIdData } from "components/pages/database/settings/unusedDatabaseIds/useUnusedDatabaseIds";

interface UsedIdProps {
    usedIdData: UsedIdData;
}

export default function UsedId({ usedIdData }: UsedIdProps) {
    const { databaseId, nodeTag, shardNumber } = usedIdData;

    return (
        <div className="used-id-pill">
            <Button
                className="text-truncate rounded-pill"
                title="Copy ID"
                color="dark"
                onClick={() => copyToClipboard.copy(databaseId, `Copied ${databaseId} vector to clipboard`)}
            >
                {databaseId}
            </Button>
            <div className="d-flex gap-1 ms-auto">
                <span className="text-muted" title={`Node ${nodeTag}`}>
                    <Icon icon="node" color="node" />
                    {nodeTag}
                </span>
                {shardNumber && (
                    <span className="text-muted" title={`Shard ${shardNumber}`}>
                        <Icon icon="shard" color="shard" />
                        {shardNumber}
                    </span>
                )}
            </div>
        </div>
    );
}
