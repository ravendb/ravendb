import React, { useCallback, useState } from "react";
import { Button } from "reactstrap";
import { DndProvider } from "react-dnd";
import { HTML5Backend } from "react-dnd-html5-backend";
import { ReorderNodes, ReorderNodesControls } from "components/pages/resources/manageDatabaseGroup/ReorderNodes";
import { OrchestratorInfoComponent } from "components/pages/resources/manageDatabaseGroup/NodeInfoComponent";
import { DeletionInProgress } from "components/pages/resources/manageDatabaseGroup/DeletionInProgress";
import { useAccessManager } from "hooks/useAccessManager";
import { useEventsCollector } from "hooks/useEventsCollector";
import { useServices } from "hooks/useServices";
import app from "durandal/app";
import { NodeInfo } from "components/models/databases";
import addNewOrchestratorToDatabase from "viewmodels/resources/addNewOrchestatorToDatabaseGroup";
import shardedDatabase from "models/resources/shardedDatabase";
import { useClusterTopologyManager } from "hooks/useClusterTopologyManager";
import viewHelpers from "common/helpers/view/viewHelpers";
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

export interface OrchestratorsGroupProps {
    orchestrators: NodeInfo[];
    db: shardedDatabase;
    deletionInProgress: string[];
}

export function OrchestratorsGroup(props: OrchestratorsGroupProps) {
    const { orchestrators, deletionInProgress, db } = props;

    const [sortableMode, setSortableMode] = useState(false);

    const [fixOrder, setFixOrder] = useState(false);
    const [newOrder, setNewOrder] = useState<NodeInfo[]>(orchestrators.slice());

    const { isOperatorOrAbove } = useAccessManager();
    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();
    const { nodeTags: clusterNodeTags } = useClusterTopologyManager();

    const addNode = useCallback(() => {
        const addKeyView = new addNewOrchestratorToDatabase(db.name, orchestrators);
        app.showBootstrapDialog(addKeyView);
    }, [db, orchestrators]);

    const enableReorder = useCallback(() => setSortableMode(true), []);
    const cancelReorder = useCallback(() => setSortableMode(false), []);

    const saveNewOrder = useCallback(
        async (tagsOrder: string[], fixOrder: boolean) => {
            //TODO reportEvent("db-group", "save-order");
            await databasesService.reorderNodesInGroup(db, tagsOrder, fixOrder);
            setSortableMode(false);
        },
        [databasesService, db, reportEvent]
    );

    const deleteOrchestratorFromGroup = useCallback(
        (nodeTag: string) => {
            viewHelpers
                .confirmationMessage("Are you sure", "Do you want to delete orchestrator from node: " + nodeTag + "?", {
                    buttons: ["Cancel", "Yes, delete"],
                    html: true,
                })
                .done((result) => {
                    if (result.can) {
                        // noinspection JSIgnoredPromiseFromCall
                        databasesService.deleteOrchestratorFromNode(db, nodeTag);
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

    const canSort = orchestrators.length === 1 || !isOperatorOrAbove();
    const existingTags = orchestrators ? orchestrators.map((x) => x.tag) : [];
    const addNodeEnabled = isOperatorOrAbove() && clusterNodeTags.some((x) => !existingTags.includes(x));

    return (
        <RichPanel className="mt-3">
            <RichPanelHeader className="bg-faded-orchestrator">
                <RichPanelInfo>
                    <RichPanelName className="text-orchestrator">
                        <i className="icon-orchestrator me-2" /> Orchestrators
                    </RichPanelName>
                </RichPanelInfo>
                <RichPanelActions>
                    <ReorderNodesControls
                        enableReorder={enableReorder}
                        canSort={canSort}
                        sortableMode={sortableMode}
                        cancelReorder={cancelReorder}
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
                            <DatabaseGroupItem className="item-new">
                                <DatabaseGroupNode>
                                    <i className="icon-node-add" />
                                </DatabaseGroupNode>
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
                            {orchestrators.map((node) => (
                                <OrchestratorInfoComponent
                                    key={node.tag}
                                    node={node}
                                    deleteFromGroup={deleteOrchestratorFromGroup}
                                />
                            ))}

                            {deletionInProgress.map((deleting) => (
                                <DeletionInProgress key={deleting} nodeTag={deleting} />
                            ))}
                        </DatabaseGroupList>
                    </DatabaseGroup>
                </React.Fragment>
            )}
        </RichPanel>
    );
}
