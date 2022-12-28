import React, { useCallback, useState } from "react";
import { FlexGrow } from "components/common/FlexGrow";
import { Button } from "reactstrap";
import { DndProvider } from "react-dnd";
import { HTML5Backend } from "react-dnd-html5-backend";
import { ReorderNodes } from "components/pages/resources/manageDatabaseGroup/ReorderNodes";
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

export interface OrchestratorsGroupProps {
    orchestrators: NodeInfo[];
    db: shardedDatabase;
    deletionInProgress: string[];
}

export function OrchestratorsGroup(props: OrchestratorsGroupProps) {
    const { orchestrators, deletionInProgress, db } = props;
    const [sortableMode, setSortableMode] = useState(false);
    const { isOperatorOrAbove } = useAccessManager();
    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();
    const { nodeTags: clusterNodeTags } = useClusterTopologyManager();

    const addNode = useCallback(() => {
        const addKeyView = new addNewOrchestratorToDatabase(db.name, orchestrators);
        app.showBootstrapDialog(addKeyView);
    }, [db, orchestrators]);

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

    const existingTags = orchestrators ? orchestrators.map((x) => x.tag) : [];
    const addNodeEnabled = isOperatorOrAbove() && clusterNodeTags.some((x) => !existingTags.includes(x));

    return (
        <div>
            <div className="d-flex mt-5">
                <span>Orchestrators</span>
            </div>

            {sortableMode ? (
                <DndProvider backend={HTML5Backend}>
                    <ReorderNodes nodes={orchestrators} saveNewOrder={saveNewOrder} cancelReorder={cancelReorder} />
                </DndProvider>
            ) : (
                <React.Fragment>
                    <div className="d-flex">
                        <FlexGrow />
                        <Button
                            disabled={orchestrators.length === 1 || !isOperatorOrAbove()}
                            onClick={enableNodesSort}
                            className="me-2"
                        >
                            <i className="icon-reorder me-1" /> Reorder orchestrators
                        </Button>
                        <Button className="me-2" color="primary" disabled={!addNodeEnabled} onClick={addNode}>
                            <i className="icon-plus me-1" />
                            Add node
                        </Button>
                    </div>

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
                </React.Fragment>
            )}
        </div>
    );
}
