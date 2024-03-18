import React, { useCallback } from "react";
import { Button } from "reactstrap";
import { DndProvider } from "react-dnd";
import { HTML5Backend } from "react-dnd-html5-backend";
import {
    ReorderNodes,
    ReorderNodesControls,
} from "components/pages/resources/manageDatabaseGroup/partials/ReorderNodes";
import { ShardInfoComponent } from "components/pages/resources/manageDatabaseGroup/partials/NodeInfoComponent";
import { DeletionInProgress } from "components/pages/resources/manageDatabaseGroup/partials/DeletionInProgress";
import { useEventsCollector } from "hooks/useEventsCollector";
import { useServices } from "hooks/useServices";
import app from "durandal/app";
import { DatabaseSharedInfo } from "components/models/databases";
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
import DatabaseUtils from "components/utils/DatabaseUtils";
import { Icon } from "components/common/Icon";
import useConfirm from "components/common/ConfirmDialog";

export interface ShardsGroupProps {
    db: DatabaseSharedInfo;
}

export function ShardsGroup(props: ShardsGroupProps) {
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
    const confirm = useConfirm();

    const addNode = useCallback(() => {
        const addKeyView = new addNewNodeToDatabaseGroup(db.name, db.nodes, db.encrypted);
        app.showBootstrapDialog(addKeyView);
    }, [db]);

    const saveNewOrder = useCallback(
        async (tagsOrder: string[], fixOrder: boolean) => {
            reportEvent("db-group", "save-order");
            await databasesService.reorderNodesInGroup(db.name, tagsOrder, fixOrder);
            exitReorder();
        },
        [databasesService, db.name, reportEvent, exitReorder]
    );

    const deleteNodeFromGroup = useCallback(
        async (nodeTag: string, hardDelete: boolean) => {
            const isConfirmed = await confirm({
                icon: "trash",
                title: (
                    <span>
                        Do you want to delete shard #<strong>{DatabaseUtils.shardNumber(db.name)}</strong> from node{" "}
                        <strong>{nodeTag}</strong>?
                    </span>
                ),
                confirmText: "Delete",
                actionColor: "danger",
            });

            if (isConfirmed) {
                await databasesService.deleteDatabaseFromNode(db.name, [nodeTag], hardDelete);
            }
        },
        [confirm, db.name, databasesService]
    );

    const onSave = async () => {
        await saveNewOrder(
            newOrder.map((x) => x.tag),
            fixOrder
        );
    };

    const shardName = DatabaseUtils.shardNumber(db.name);

    return (
        <RichPanel className="mt-3">
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelName>
                        <Icon icon="shard" color="shard" /> Shard #{shardName}
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
                                        <Icon icon="plus" />
                                        Add node
                                    </Button>
                                </DatabaseGroupActions>
                            </DatabaseGroupItem>

                            {db.nodes.map((node) => (
                                <ShardInfoComponent
                                    key={node.tag}
                                    node={node}
                                    deleteFromGroup={deleteNodeFromGroup}
                                    db={db}
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
