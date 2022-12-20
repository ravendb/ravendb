import React, { useCallback, useState } from "react";
import { FlexGrow } from "components/common/FlexGrow";
import { Button } from "reactstrap";
import { DndProvider } from "react-dnd";
import { HTML5Backend } from "react-dnd-html5-backend";
import { ReorderNodes } from "components/pages/resources/manageDatabaseGroup/ReorderNodes";
import { NodeInfoComponent } from "components/pages/resources/manageDatabaseGroup/NodeInfoComponent";
import { DeletionInProgress } from "components/pages/resources/manageDatabaseGroup/DeletionInProgress";
import { useAccessManager } from "hooks/useAccessManager";
import { NodeInfo } from "components/pages/resources/manageDatabaseGroup/types";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import database from "models/resources/database";
import { useEventsCollector } from "hooks/useEventsCollector";
import { useServices } from "hooks/useServices";
import addNewNodeToDatabaseGroup from "viewmodels/resources/addNewNodeToDatabaseGroup";
import app from "durandal/app";
import clusterTopologyManager from "common/shell/clusterTopologyManager";

export interface NodeGroupProps {
    nodes: NodeInfo[];
    db: database;
    lockMode: DatabaseLockMode;
    deletionInProgress: string[];
    refresh: () => void;
}

export function NodeGroup(props: NodeGroupProps) {
    const { nodes, deletionInProgress, db, lockMode, refresh } = props;
    const [sortableMode, setSortableMode] = useState(false);
    const { isOperatorOrAbove } = useAccessManager();
    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();

    const addNode = useCallback(() => {
        const addKeyView = new addNewNodeToDatabaseGroup(db.name, nodes, db.isEncrypted());
        app.showBootstrapDialog(addKeyView);
    }, [db, nodes]);

    const enableNodesSort = useCallback(() => {
        setSortableMode(true);
    }, []);

    const cancelReorder = useCallback(() => {
        setSortableMode(false);
    }, []);

    const saveNewOrder = useCallback(
        async (tagsOrder: string[], fixOrder: boolean) => {
            reportEvent("db-group", "save-order");
            await databasesService.reorderNodesInGroup(db, tagsOrder, fixOrder);
            setSortableMode(false);
            refresh();
        },
        [databasesService, db, reportEvent, refresh]
    );

    const clusterTopology = clusterTopologyManager.default.topology();
    const clusterNodeTags = clusterTopology.nodes().map((x) => x.tag());
    const existingTags = nodes ? nodes.map((x) => x.tag) : [];
    const addNodeEnabled = isOperatorOrAbove() && clusterNodeTags.some((x) => !existingTags.includes(x));

    return (
        <div>
            <div className="d-flex">
                <span>Database Group:</span>
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
                        <NodeInfoComponent key={node.tag} node={node} db={db} databaseLockMode={lockMode} />
                    ))}

                    {deletionInProgress.map((deleting) => (
                        <DeletionInProgress key={deleting} nodeTag={deleting} />
                    ))}
                </React.Fragment>
            )}
        </div>
    );
}
