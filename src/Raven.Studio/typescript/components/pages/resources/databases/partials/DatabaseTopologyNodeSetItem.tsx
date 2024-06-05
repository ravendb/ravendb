import React from "react";
import { locationAwareLoadableData } from "components/models/common";
import { NodeInfo, DatabaseLocalInfo } from "components/models/databases";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databaseLocationComparator } from "components/utils/common";
import { DatabaseNodeSetItem } from "./DatabaseNodeSetItem";

interface DatabaseTopologyNodeSetItemProps {
    node: NodeInfo;
    localInfos: locationAwareLoadableData<DatabaseLocalInfo>[];
    isDisabled: boolean;
    shardNumber?: number;
}

export default function DatabaseTopologyNodeSetItem({
    node,
    localInfos,
    isDisabled,
    shardNumber,
}: DatabaseTopologyNodeSetItemProps) {
    const localInfo = localInfos.find((x) =>
        shardNumber != null
            ? databaseLocationComparator(x.location, {
                  nodeTag: node.tag,
                  shardNumber,
              })
            : x.location.nodeTag === node.tag
    );

    if (!localInfo) {
        return null;
    }

    const isOffline = localInfo.data ? DatabaseUtils.formatUptime(localInfo.data.upTime) === "Offline" : false;

    const isInactive = localInfo.status === "idle" || localInfo.status === "loading" || isOffline || isDisabled;

    return <DatabaseNodeSetItem node={node} isInactive={isInactive} />;
}
