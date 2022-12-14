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

    const [state, dispatch] = useReducer(manageDatabaseGroupReducer, null); // TODO initial state?

    const { databasesService } = useServices();

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

    const settingsUniqueId = useId("settings");

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
    const addNodeEnabled = isOperatorOrAbove() && clusterNodeTags.some((x) => !existingTags.includes(x));

    const dynamicDatabaseDistributionWarning = getDynamicDatabaseDistributionWarning(
        licenseStatus,
        state.encrypted,
        state.nodes.length
    );
    const enableDynamicDatabaseDistribution = isOperatorOrAbove() && !dynamicDatabaseDistributionWarning;

    //TODO: review data-bind

    return (
        <div className="content-margin">
            <div className="sticky-header">
                <Button
                    className="me-2"
                    data-bind="click: enableNodesSort, enable: nodes().length > 1, requiredAccess: 'Operator'"
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
                            <div className={"form-switch form-check-reverse"}>
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
