import React, { useCallback, useEffect, useReducer, useState } from "react";
import { Button, Input, Label, Spinner } from "reactstrap";
import { UncontrolledButtonWithDropdownPanel } from "components/common/DropdownPanel";
import useId from "hooks/useId";
import useBoolean from "hooks/useBoolean";
import { useServices } from "hooks/useServices";
import { NodeInfoComponent } from "components/pages/resources/manageDatabaseGroup/NodeInfoComponent";
import { manageDatabaseGroupReducer } from "components/pages/resources/manageDatabaseGroup/reducer";
import database from "models/resources/database";
import addNewNodeToDatabaseGroup from "viewmodels/resources/addNewNodeToDatabaseGroup";
import app from "durandal/app";
import { useLicenseStatus } from "hooks/useLicenseStatus";
import LicenseStatus = Raven.Server.Commercial.LicenseStatus;
import { DeletionInProgress } from "components/pages/resources/manageDatabaseGroup/DeletionInProgress";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import clusterTopology from "models/database/cluster/clusterTopology";
import { useChanges } from "hooks/useChanges";
import { useAccessManager } from "hooks/useAccessManager";
import { ReorderNodes } from "components/pages/resources/manageDatabaseGroup/ReorderNodes";
import messagePublisher from "common/messagePublisher";
import { useEventsCollector } from "hooks/useEventsCollector";
import { HTML5Backend } from "react-dnd-html5-backend";
import { DndProvider } from "react-dnd";

interface ManageDatabaseGroupPageProps {
    db: database;
}

function anyNodeHasError(topology: clusterTopology) {
    if (!topology) {
        return true;
    }
    const votingInProgress = topology.currentState() === "Candidate";

    if (votingInProgress) {
        return true;
    }

    return topology.nodes().some((x) => !x.connected());
}

function getDynamicDatabaseDistributionWarning(
    licenseStatus: LicenseStatus,
    encryptedDatabase: boolean,
    nodesCount: number
) {
    if (!licenseStatus.HasDynamicNodesDistribution) {
        return "Your current license doesn't include the dynamic nodes distribution feature.";
    }

    if (encryptedDatabase) {
        return "Dynamic database distribution is not available when database is encrypted.";
    }

    if (nodesCount === 1) {
        return "There is only one node in the group.";
    }

    return null;
}

export function ManageDatabaseGroupPage(props: ManageDatabaseGroupPageProps) {
    const { db } = props;

    const [sortableMode, setSortableMode] = useState(false);

    const [state, dispatch] = useReducer(manageDatabaseGroupReducer, null);

    const { databasesService } = useServices();

    const { reportEvent } = useEventsCollector();

    const { status: licenseStatus } = useLicenseStatus();

    const clusterTopology = clusterTopologyManager.default.topology();

    const { isOperatorOrAbove } = useAccessManager();

    const {
        value: dynamicDatabaseDistribution,
        toggle: toggleDynamicDatabaseDistribution,
        setValue: setDynamicDatabaseDistribution,
    } = useBoolean(false);

    //TODO: error handling!
    const fetchDatabaseInfo = useCallback(
        async (databaseName: string) => {
            const info = await databasesService.getDatabase(databaseName);
            dispatch({
                type: "DatabaseInfoLoaded",
                info,
            });
            return info;
        },
        [databasesService]
    );

    useEffect(() => {
        const fetchData = async () => {
            if (db) {
                const dbInfo = await fetchDatabaseInfo(db.name);
                setDynamicDatabaseDistribution(dbInfo.DynamicNodesDistribution);
            }
        };

        fetchData();
    }, [fetchDatabaseInfo, db, setDynamicDatabaseDistribution]);

    const refresh = useCallback(() => {
        if (!sortableMode) {
            fetchDatabaseInfo(db.name);
        }
    }, [sortableMode, fetchDatabaseInfo, db]);

    const { serverNotifications } = useChanges();

    useEffect(() => {
        const sub = serverNotifications.watchClusterTopologyChanges(() => refresh());
        return () => sub.off();
    }, [serverNotifications, refresh]);

    useEffect(() => {
        const sub = serverNotifications.watchAllDatabaseChanges(() => refresh());
        return () => sub.off();
    }, [serverNotifications, refresh]);

    useEffect(() => {
        const sub = serverNotifications.watchReconnect(() => refresh());
        return () => sub.off();
    });

    useEffect(() => {
        const anyError = anyNodeHasError(clusterTopology);
        //TODO: test this part!
        if (sortableMode && anyError) {
            messagePublisher.reportWarning(
                "Can't reorder nodes, when at least one node is down or voting is in progress."
            );
            setSortableMode(false);
        }
    }, [clusterTopology, sortableMode]);

    const settingsUniqueId = useId("settings");

    const addNode = useCallback(() => {
        const addKeyView = new addNewNodeToDatabaseGroup(db.name, state.nodes, state.encrypted);
        app.showBootstrapDialog(addKeyView);
    }, [db, state]);

    const changeDynamicDatabaseDistribution = useCallback(async () => {
        toggleDynamicDatabaseDistribution();

        await databasesService.toggleDynamicNodeAssignment(db, !dynamicDatabaseDistribution);
    }, [dynamicDatabaseDistribution, toggleDynamicDatabaseDistribution, databasesService, db]);

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
            await fetchDatabaseInfo(db.name);
        },
        [databasesService, db, reportEvent, fetchDatabaseInfo]
    );

    if (!state) {
        return <Spinner />;
    }

    const clusterNodeTags = clusterTopology.nodes().map((x) => x.tag());
    const existingTags = state.nodes ? state.nodes.map((x) => x.tag) : [];
    const addNodeEnabled = isOperatorOrAbove() && clusterNodeTags.some((x) => !existingTags.includes(x));

    const dynamicDatabaseDistributionWarning = getDynamicDatabaseDistributionWarning(
        licenseStatus,
        state.encrypted,
        state.nodes.length
    );
    const enableDynamicDatabaseDistribution = isOperatorOrAbove() && !dynamicDatabaseDistributionWarning;

    return (
        <div className="content-margin">
            {!sortableMode && (
                <div className="sticky-header">
                    <Button
                        disabled={state.nodes.length === 1 || !isOperatorOrAbove()}
                        onClick={enableNodesSort}
                        className="me-2"
                    >
                        <i className="icon-reorder me-1" /> Reorder nodes
                    </Button>
                    <Button className="me-2" color="primary" disabled={!addNodeEnabled} onClick={addNode}>
                        <i className="icon-plus me-1" />
                        Add node to group
                    </Button>
                    <UncontrolledButtonWithDropdownPanel buttonText="Settings">
                        <>
                            <Label className="dropdown-item-text m-0" htmlFor={settingsUniqueId}>
                                <div className="form-switch form-check-reverse">
                                    <Input
                                        id={settingsUniqueId}
                                        type="switch"
                                        role="switch"
                                        disabled={!enableDynamicDatabaseDistribution}
                                        checked={dynamicDatabaseDistribution}
                                        onChange={changeDynamicDatabaseDistribution}
                                    />
                                    Allow dynamic database distribution
                                </div>
                            </Label>
                            {dynamicDatabaseDistributionWarning && (
                                <div className="bg-faded-warning px-4 py-2">{dynamicDatabaseDistributionWarning}</div>
                            )}
                        </>
                    </UncontrolledButtonWithDropdownPanel>
                </div>
            )}
            {sortableMode && (
                <DndProvider backend={HTML5Backend}>
                    <ReorderNodes nodes={state.nodes} saveNewOrder={saveNewOrder} cancelReorder={cancelReorder} />
                </DndProvider>
            )}
            {!sortableMode && (
                <div>
                    {state.nodes.map((node) => (
                        <NodeInfoComponent key={node.tag} node={node} db={db} databaseLockMode={state.lockMode} />
                    ))}

                    {state.deletionInProgress.map((deleting) => (
                        <DeletionInProgress key={deleting} nodeTag={deleting} />
                    ))}
                </div>
            )}
        </div>
    );
}
