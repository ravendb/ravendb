import React, { useCallback, useState } from "react";
import { FlexGrow } from "components/common/FlexGrow";
import { Button } from "reactstrap";
import { DndProvider } from "react-dnd";
import { HTML5Backend } from "react-dnd-html5-backend";
import { ReorderNodes } from "components/pages/resources/manageDatabaseGroup/ReorderNodes";
import { NodeInfoComponent } from "components/pages/resources/manageDatabaseGroup/NodeInfoComponent";
import { DeletionInProgress } from "components/pages/resources/manageDatabaseGroup/DeletionInProgress";
import { useAccessManager } from "hooks/useAccessManager";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import database from "models/resources/database";
import { useEventsCollector } from "hooks/useEventsCollector";
import { useServices } from "hooks/useServices";
import addNewNodeToDatabaseGroup from "viewmodels/resources/addNewNodeToDatabaseGroup";
import app from "durandal/app";
import { NodeInfo } from "components/models/databases";
import { useClusterTopologyManager } from "hooks/useClusterTopologyManager";
import viewHelpers from "common/helpers/view/viewHelpers";
import genUtils from "common/generalUtils";
import {
    RichPanel,
    RichPanelActions,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import {
    DatabaseGroup,
    DatabaseGroupActions,
    DatabaseGroupItem,
    DatabaseGroupList,
    DatabaseGroupNode,
} from "components/common/DatabaseGroup";

export interface NodeGroupProps {
    nodes: NodeInfo[];
    db: database;
    lockMode: DatabaseLockMode;
    deletionInProgress: string[];
}

export function NodeGroup(props: NodeGroupProps) {
    const { nodes, deletionInProgress, db, lockMode } = props;
    const [sortableMode, setSortableMode] = useState(false);
    const { isOperatorOrAbove } = useAccessManager();
    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();
    const { nodeTags: clusterNodeTags } = useClusterTopologyManager();

    const addNode = useCallback(() => {
        const addKeyView = new addNewNodeToDatabaseGroup(db.name, nodes, db.isEncrypted());
        app.showBootstrapDialog(addKeyView);
    }, [db, nodes]);

    const enableNodesSort = useCallback(() => setSortableMode(true), []);

    const cancelReorder = useCallback(() => setSortableMode(false), []);

    const saveNewOrder = useCallback(
        async (tagsOrder: string[], fixOrder: boolean) => {
            reportEvent("db-group", "save-order");
            await databasesService.reorderNodesInGroup(db, tagsOrder, fixOrder);
            setSortableMode(false);
        },
        [databasesService, db, reportEvent]
    );

    const deleteNodeFromGroup = useCallback(
        (nodeTag: string, hardDelete: boolean) => {
            viewHelpers
                .confirmationMessage(
                    "Are you sure",
                    "Do you want to delete database '" + genUtils.escapeHtml(db.name) + "' from node: " + nodeTag + "?",
                    {
                        buttons: ["Cancel", "Yes, delete"],
                        html: true,
                    }
                )
                .done((result) => {
                    if (result.can) {
                        // noinspection JSIgnoredPromiseFromCall
                        databasesService.deleteDatabaseFromNode(db, [nodeTag], hardDelete);
                    }
                });
        },
        [db, databasesService]
    );

    const existingTags = nodes ? nodes.map((x) => x.tag) : [];
    const addNodeEnabled = isOperatorOrAbove() && clusterNodeTags.some((x) => !existingTags.includes(x));

    return (
        <RichPanel className="mt-3">
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelName>
                        <i className="icon-dbgroup me-2" /> Database Group
                    </RichPanelName>
                </RichPanelInfo>
                <RichPanelActions>
                    {!sortableMode && (
                        <Button
                            disabled={nodes.length === 1 || !isOperatorOrAbove()}
                            onClick={enableNodesSort}
                            className="me-2"
                        >
                            <i className="icon-reorder me-1" /> Reorder nodes
                        </Button>
                    )}
                </RichPanelActions>
            </RichPanelHeader>

            <DatabaseGroup>
                <div className="dbgroup-image"></div>
                <DatabaseGroupList>
                    {sortableMode ? (
                        <DndProvider backend={HTML5Backend}>
                            <ReorderNodes nodes={nodes} saveNewOrder={saveNewOrder} cancelReorder={cancelReorder} />
                        </DndProvider>
                    ) : (
                        <React.Fragment>
                            <DatabaseGroupItem className="item-new">
                                <DatabaseGroupNode icon="node-add" color="success" />

                                <DatabaseGroupActions>
                                    <Button
                                        size="xs"
                                        color="success"
                                        outline
                                        className="rounded-pill"
                                        disabled={!addNodeEnabled}
                                        onClick={addNode}
                                    >
                                        <i className="icon-plus me-1" />
                                        Add node
                                    </Button>
                                </DatabaseGroupActions>
                            </DatabaseGroupItem>

                            {nodes.map((node) => (
                                <NodeInfoComponent
                                    key={node.tag}
                                    node={node}
                                    databaseLockMode={lockMode}
                                    deleteFromGroup={deleteNodeFromGroup}
                                />
                            ))}

                            {deletionInProgress.map((deleting) => (
                                <DeletionInProgress key={deleting} nodeTag={deleting} />
                            ))}
                        </React.Fragment>
                    )}
                </DatabaseGroupList>
            </DatabaseGroup>
        </RichPanel>
    );
}
