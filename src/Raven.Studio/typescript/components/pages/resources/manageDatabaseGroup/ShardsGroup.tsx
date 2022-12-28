import React, { useCallback, useState } from "react";
import { FlexGrow } from "components/common/FlexGrow";
import { Button } from "reactstrap";
import { DndProvider } from "react-dnd";
import { HTML5Backend } from "react-dnd-html5-backend";
import { ReorderNodes } from "components/pages/resources/manageDatabaseGroup/ReorderNodes";
import { ShardInfoComponent } from "components/pages/resources/manageDatabaseGroup/NodeInfoComponent";
import { useAccessManager } from "hooks/useAccessManager";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { useEventsCollector } from "hooks/useEventsCollector";
import { useServices } from "hooks/useServices";
import app from "durandal/app";
import { NodeInfo } from "components/models/databases";
import { useClusterTopologyManager } from "hooks/useClusterTopologyManager";
import shard = require("models/resources/shard");
import viewHelpers from "common/helpers/view/viewHelpers";
import genUtils from "common/generalUtils";
import addNewNodeToDatabaseGroup from "viewmodels/resources/addNewNodeToDatabaseGroup";

export interface ShardsGroupProps {
    nodes: NodeInfo[];
    shard: shard;
    lockMode: DatabaseLockMode;
}

//TODO: deletion in progress?

export function ShardsGroup(props: ShardsGroupProps) {
    const { nodes, lockMode, shard } = props;

    const [sortableMode, setSortableMode] = useState(false);
    const { isOperatorOrAbove } = useAccessManager();
    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();
    const { nodeTags: clusterNodeTags } = useClusterTopologyManager();

    const addNode = useCallback(() => {
        //TODO: what if db is encrypted?
        const addKeyView = new addNewNodeToDatabaseGroup(shard.name, nodes, shard.isEncrypted());
        app.showBootstrapDialog(addKeyView);
    }, [shard, nodes]);

    const enableNodesSort = useCallback(() => setSortableMode(true), []);

    const cancelReorder = useCallback(() => setSortableMode(false), []);

    const saveNewOrder = useCallback(
        async (tagsOrder: string[]) => {
            reportEvent("db-group", "save-order");
            await databasesService.reorderShardsInGroup(shard, tagsOrder);
            setSortableMode(false);
        },
        [databasesService, shard, reportEvent]
    );

    const deleteNodeFromGroup = useCallback(
        (nodeTag: string, hardDelete: boolean) => {
            viewHelpers
                .confirmationMessage(
                    "Are you sure",
                    "Do you want to delete '" + genUtils.escapeHtml(shard.shardName) + "' from node: " + nodeTag + "?",
                    {
                        buttons: ["Cancel", "Yes, delete"],
                        html: true,
                    }
                )
                .done((result) => {
                    if (result.can) {
                        // noinspection JSIgnoredPromiseFromCall
                        databasesService.deleteDatabaseFromNode(shard, [nodeTag], hardDelete);
                    }
                });
        },
        [shard, databasesService]
    );

    const existingTags = nodes ? nodes.map((x) => x.tag) : [];
    const addNodeEnabled = isOperatorOrAbove() && clusterNodeTags.some((x) => !existingTags.includes(x));

    return (
        <div className="mt-5">
            <div className="d-flex">
                <span>{shard.shardName}</span>
            </div>

            {sortableMode ? (
                <DndProvider backend={HTML5Backend}>
                    <ReorderNodes nodes={nodes} saveNewOrder={saveNewOrder} cancelReorder={cancelReorder} />
                </DndProvider>
            ) : (
                <React.Fragment>
                    <div className="d-flex">
                        <FlexGrow />
                        <Button
                            disabled={nodes.length === 1 || !isOperatorOrAbove()}
                            onClick={enableNodesSort}
                            className="me-2"
                        >
                            <i className="icon-reorder me-1" /> Reorder nodes
                        </Button>
                        <Button className="me-2" color="primary" disabled={!addNodeEnabled} onClick={addNode}>
                            <i className="icon-plus me-1" />
                            Add node
                        </Button>
                    </div>

                    {nodes.map((node) => (
                        <ShardInfoComponent
                            key={node.tag}
                            node={node}
                            databaseLockMode={lockMode}
                            deleteFromGroup={deleteNodeFromGroup}
                        />
                    ))}
                </React.Fragment>
            )}
        </div>
    );
}
