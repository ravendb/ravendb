import { DatabaseLocalInfo, DatabaseSharedInfo, ShardedDatabaseSharedInfo } from "components/models/databases";
import { NodeSet, NodeSetItem, NodeSetLabel } from "components/common/NodeSet";
import React from "react";
import DatabaseUtils from "components/utils/DatabaseUtils";
import DatabaseTopologyNodeSetItem from "./DatabaseTopologyNodeSetItem";
import { locationAwareLoadableData } from "components/models/common";

interface DatabaseTopologyProps {
    db: DatabaseSharedInfo;
    localInfos: locationAwareLoadableData<DatabaseLocalInfo>[];
    togglePanelCollapsed: () => void;
}

export function DatabaseTopology(props: DatabaseTopologyProps) {
    const { db, localInfos, togglePanelCollapsed } = props;

    if (db.sharded) {
        const shardedDb = db as ShardedDatabaseSharedInfo;
        return (
            <div>
                <NodeSet
                    color="orchestrator"
                    className="m-1 cursor-pointer"
                    onClick={togglePanelCollapsed}
                    title="Expand distribution details"
                >
                    <NodeSetLabel color="orchestrator" icon="orchestrator">
                        Orchestrators
                    </NodeSetLabel>
                    {db.nodes.map((node) => (
                        <DatabaseTopologyNodeSetItem key={node.tag} node={node} localInfos={localInfos} />
                    ))}
                </NodeSet>

                {shardedDb.shards.map((shard) => {
                    const shardNumber = DatabaseUtils.shardNumber(shard.name);
                    return (
                        <React.Fragment key={shard.name}>
                            <NodeSet
                                color="shard"
                                className="m-1 cursor-pointer"
                                onClick={togglePanelCollapsed}
                                title="Expand distribution details"
                            >
                                <NodeSetLabel color="shard" icon="shard">
                                    #{shardNumber}
                                </NodeSetLabel>
                                {db.nodes.map((node) => (
                                    <DatabaseTopologyNodeSetItem
                                        key={node.tag}
                                        node={node}
                                        localInfos={localInfos}
                                        shardNumber={shardNumber}
                                    />
                                ))}
                                {shard.deletionInProgress.map((node) => {
                                    return (
                                        <NodeSetItem
                                            key={"deletion-" + node}
                                            icon="trash"
                                            color="warning"
                                            title="Deletion in progress"
                                            extraIconClassName="pulse"
                                        >
                                            {node}
                                        </NodeSetItem>
                                    );
                                })}
                            </NodeSet>
                        </React.Fragment>
                    );
                })}
            </div>
        );
    } else {
        return (
            <div>
                <NodeSet
                    className="m-1 cursor-pointer"
                    onClick={togglePanelCollapsed}
                    title="Expand distribution details"
                >
                    <NodeSetLabel icon="database">Nodes</NodeSetLabel>
                    {db.nodes.map((node) => (
                        <DatabaseTopologyNodeSetItem key={node.tag} node={node} localInfos={localInfos} />
                    ))}
                    {db.deletionInProgress.map((node) => {
                        return (
                            <NodeSetItem
                                key={"deletion-" + node}
                                icon="trash"
                                color="warning"
                                title="Deletion in progress"
                                extraIconClassName="pulse"
                            >
                                {node}
                            </NodeSetItem>
                        );
                    })}
                </NodeSet>
            </div>
        );
    }
}
