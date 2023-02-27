import React, { useCallback } from "react";
import { Button } from "reactstrap";
import { DndProvider } from "react-dnd";
import { HTML5Backend } from "react-dnd-html5-backend";
import {
    ReorderNodes,
    ReorderNodesControls,
} from "components/pages/resources/manageDatabaseGroup/partials/ReorderNodes";
import { NodeInfoComponent } from "components/pages/resources/manageDatabaseGroup/partials/NodeInfoComponent";
import { DeletionInProgress } from "components/pages/resources/manageDatabaseGroup/partials/DeletionInProgress";
import { useEventsCollector } from "hooks/useEventsCollector";
import { useServices } from "hooks/useServices";
import app from "durandal/app";
import { DatabaseSharedInfo } from "components/models/databases";
import viewHelpers from "common/helpers/view/viewHelpers";
import genUtils from "common/generalUtils";
import addNewNodeToDatabaseGroup from "viewmodels/resources/addNewNodeToDatabaseGroup";
import classNames from "classnames";
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
import { useGroup } from "components/pages/resources/manageDatabaseGroup/partials/useGroup";

export interface NodeGroupProps {
    db: DatabaseSharedInfo;
}

export function NodeGroup(props: NodeGroupProps) {
    const { db } = props;

    const {
        fixOrder,
        setNewOrder,
        newOrder,
        setFixOrder,
        addNodeEnabled,
        canSort,
        sortableMode,
        enableReorder,
        exitReorder,
    } = useGroup(db.nodes, db.fixOrder);

    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();

    const addNode = useCallback(() => {
        const addKeyView = new addNewNodeToDatabaseGroup(db.name, db.nodes, db.encrypted);
        app.showBootstrapDialog(addKeyView);
    }, [db]);

    const saveNewOrder = useCallback(
        async (tagsOrder: string[], fixOrder: boolean) => {
            reportEvent("db-group", "save-order");
            await databasesService.reorderNodesInGroup(db, tagsOrder, fixOrder);
            exitReorder();
        },
        [databasesService, db, reportEvent, exitReorder]
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

    const onSave = async () => {
        await saveNewOrder(
            newOrder.map((x) => x.tag),
            fixOrder
        );
    };

    return (
        <RichPanel className="mt-3">
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelName>
                        <i className="icon-dbgroup me-2" /> Database Group
                    </RichPanelName>
                </RichPanelInfo>
                <RichPanelActions>
                    <ReorderNodesControls
                        enableReorder={enableReorder}
                        canSort={canSort}
                        sortableMode={sortableMode}
                        cancelReorder={exitReorder}
                        onSave={onSave}
                    />
                </RichPanelActions>
            </RichPanelHeader>

            {sortableMode ? (
                <DndProvider backend={HTML5Backend}>
                    <ReorderNodes
                        fixOrder={fixOrder}
                        setFixOrder={setFixOrder}
                        newOrder={newOrder}
                        setNewOrder={setNewOrder}
                    />
                </DndProvider>
            ) : (
                <React.Fragment>
                    <DatabaseGroup>
                        <DatabaseGroupList>
                            <DatabaseGroupItem
                                className={classNames("item-new", "position-relative", {
                                    "item-disabled": !addNodeEnabled,
                                })}
                            >
                                <DatabaseGroupNode icon="node-add" color="success" />
                                <DatabaseGroupActions>
                                    <Button
                                        size="xs"
                                        color="success"
                                        outline
                                        className="rounded-pill stretched-link"
                                        disabled={!addNodeEnabled}
                                        onClick={addNode}
                                    >
                                        <i className="icon-plus me-1" />
                                        Add node
                                    </Button>
                                </DatabaseGroupActions>
                            </DatabaseGroupItem>

                            {db.nodes.map((node) => (
                                <NodeInfoComponent
                                    key={node.tag}
                                    node={node}
                                    databaseLockMode={db.lockMode}
                                    deleteFromGroup={deleteNodeFromGroup}
                                />
                            ))}

                            {db.deletionInProgress.map((deleting) => (
                                <DeletionInProgress key={deleting} nodeTag={deleting} />
                            ))}
                        </DatabaseGroupList>
                    </DatabaseGroup>
                </React.Fragment>
            )}
        </RichPanel>
    );
}
