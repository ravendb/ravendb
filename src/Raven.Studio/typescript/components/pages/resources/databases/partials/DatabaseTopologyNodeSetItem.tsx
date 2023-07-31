import React from "react";
import { locationAwareLoadableData } from "components/models/common";
import { NodeInfo, DatabaseLocalInfo } from "components/models/databases";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databaseLocationComparator } from "components/utils/common";
import { DatabaseNodeSetItem } from "./DatabaseNodeSetItem";

interface DatabaseTopologyNodeSetItemProps {
    node: NodeInfo;
    dbState: locationAwareLoadableData<DatabaseLocalInfo>[];
    shardNumber?: number;
}

export default function DatabaseTopologyNodeSetItem({ node, dbState, shardNumber }: DatabaseTopologyNodeSetItemProps) {
    const localInfo = dbState.find((x) =>
        databaseLocationComparator(x.location, {
            nodeTag: node.tag,
            shardNumber,
        })
    );

    const isOffline = DatabaseUtils.formatUptime(localInfo?.data?.upTime) === "Offline";

    return <DatabaseNodeSetItem node={node} isOffline={isOffline} />;
}
