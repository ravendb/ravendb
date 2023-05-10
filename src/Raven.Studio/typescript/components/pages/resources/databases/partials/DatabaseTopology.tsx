import { DatabaseSharedInfo, ShardedDatabaseSharedInfo } from "components/models/databases";
import { NodeSet, NodeSetItem, NodeSetLabel } from "components/common/NodeSet";
import { DatabaseNodeSetItem } from "components/pages/resources/databases/partials/DatabaseNodeSetItem";
import React from "react";
import DatabaseUtils from "components/utils/DatabaseUtils";

interface DatabaseTopologyProps {
    db: DatabaseSharedInfo;
    togglePanelCollapsed: () => void;
}

export function DatabaseTopology(props: DatabaseTopologyProps) {
    const { db, togglePanelCollapsed } = props;

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
                        <DatabaseNodeSetItem key={node.tag} node={node} />
                    ))}
                </NodeSet>

                {shardedDb.shards.map((shard) => {
                    return (
                        <React.Fragment key={shard.name}>
                            <NodeSet
                                color="shard"
                                className="m-1 cursor-pointer"
                                onClick={togglePanelCollapsed}
                                title="Expand distribution details"
                            >
                                <NodeSetLabel color="shard" icon="shard">
                                    #{DatabaseUtils.shardNumber(shard.name)}
                                </NodeSetLabel>
                                {shard.nodes.map((node) => (
                                    <DatabaseNodeSetItem key={node.tag} node={node} />
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
                    {db.nodes.map((node) => {
                        return <DatabaseNodeSetItem key={node.tag} node={node} />;
                    })}
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
