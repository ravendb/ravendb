import React, { useCallback, useEffect, useReducer, useState } from "react";
import { Alert, Button, FormGroup, Input, Label, Spinner } from "reactstrap";
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

// eslint-disable-next-line @typescript-eslint/no-empty-interface
interface ManageDatabaseGroupPageProps {
    db: database;
    //TODO:
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

    //TODO: reorder nodes

    //TODO: permissions! (Requried role etc)

    const [state, dispatch] = useReducer(manageDatabaseGroupReducer, null); // TODO initial state?

    const { databasesService } = useServices();

    const { status: licenseStatus } = useLicenseStatus();

    const clusterTopology = clusterTopologyManager.default.topology();

    //TODO: error handling!
    const fetchDatabaseInfo = useCallback(
        async (databaseName: string) => {
            const info = await databasesService.getDatabase(databaseName);
            dispatch({
                type: "DatabaseInfoLoaded",
                info,
            });
        },
        [databasesService]
    );

    useEffect(() => {
        if (db) {
            fetchDatabaseInfo(db.name);
        }
    }, [fetchDatabaseInfo, db]);

    const refresh = useCallback(() => {
        //TODO bind this event
        if (!sortableMode) {
            fetchDatabaseInfo(db.name);
        }
    }, [sortableMode, fetchDatabaseInfo, db]);

    const { serverNotifications } = useChanges();

    /* TODO
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
    });*/

    const settingsUniqueId = useId("settings");

    const { value: dynamicDatabaseDistribution, toggle: toggleDynamicDatabaseDistribution } = useBoolean(false); //TODO: init with value!

    const addNode = useCallback(() => {
        const addKeyView = new addNewNodeToDatabaseGroup(db.name, state.nodes, state.encrypted);
        app.showBootstrapDialog(addKeyView);
    }, [db, state]);

    const changeDynamicDatabaseDistribution = useCallback(async () => {
        toggleDynamicDatabaseDistribution();

        await databasesService.toggleDynamicNodeAssignment(db, !dynamicDatabaseDistribution);
    }, [dynamicDatabaseDistribution, toggleDynamicDatabaseDistribution, databasesService, db]);

    if (!state) {
        return <Spinner />;
    }

    const clusterNodeTags = clusterTopology.nodes().map((x) => x.tag());
    const existingTags = state.nodes ? state.nodes.map((x) => x.tag) : [];
    const addNodeEnabled = clusterNodeTags.some((x) => !existingTags.includes(x));

    const dynamicDatabaseDistributionWarning = getDynamicDatabaseDistributionWarning(
        licenseStatus,
        state.encrypted,
        state.nodes.length
    );
    const enableDynamicDatabaseDistribution = !dynamicDatabaseDistributionWarning;

    return (
        <div className="content-margin">
            <div>
                <Button data-bind="click: enableNodesSort, enable: nodes().length > 1, requiredAccess: 'Operator'">
                    <i className="icon-reorder"></i> Reorder nodes
                </Button>
                <Button
                    color="primary"
                    disabled={!addNodeEnabled}
                    onClick={addNode}
                    data-bind="requiredAccess: 'Operator'"
                >
                    <i className="icon-plus"></i>
                    <span>Add node to group</span>
                </Button>
                <UncontrolledButtonWithDropdownPanel buttonText="Settings">
                    <div>
                        <FormGroup switch className="form-check-reverse">
                            <Input
                                id={settingsUniqueId}
                                type="switch"
                                role="switch"
                                disabled={!enableDynamicDatabaseDistribution}
                                checked={dynamicDatabaseDistribution}
                                onChange={changeDynamicDatabaseDistribution}
                                data-bind="checked: dynamicDatabaseDistribution, enable: enableDynamicDatabaseDistribution, requiredAccess: 'Operator', requiredAccessOptions: { strategy: 'disable' }"
                            />
                            <Label htmlFor={settingsUniqueId} check>
                                Allow dynamic database distribution
                            </Label>

                            {dynamicDatabaseDistributionWarning && (
                                <Alert color="warning">{dynamicDatabaseDistributionWarning}</Alert>
                            )}
                        </FormGroup>
                    </div>
                </UncontrolledButtonWithDropdownPanel>
            </div>
            <div>
                {state.nodes.map((node) => (
                    <NodeInfoComponent key={node.tag} node={node} db={db} databaseLockMode={state.lockMode} />
                ))}

                {state.deletionInProgress.map((deleting) => (
                    <DeletionInProgress key={deleting} nodeTag={deleting} />
                ))}
            </div>
        </div>
    );
}

/* todo
    this.anyNodeHasError.subscribe((error) => {
        if (error && this.inSortableMode()) {
            messagePublisher.reportWarning("Can't reorder nodes, when at least one node is down or voting is in progress.");
            this.cancelReorder();
        }
    }));
 */
