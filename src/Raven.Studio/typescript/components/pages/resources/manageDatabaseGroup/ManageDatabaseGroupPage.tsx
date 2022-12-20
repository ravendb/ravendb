import React, { useCallback, useEffect, useReducer } from "react";
import { Input, Label, Spinner } from "reactstrap";
import { UncontrolledButtonWithDropdownPanel } from "components/common/DropdownPanel";
import useId from "hooks/useId";
import useBoolean from "hooks/useBoolean";
import { useServices } from "hooks/useServices";
import { manageDatabaseGroupReducer } from "components/pages/resources/manageDatabaseGroup/reducer";
import database from "models/resources/database";
import { useLicenseStatus } from "hooks/useLicenseStatus";
import LicenseStatus = Raven.Server.Commercial.LicenseStatus;
import clusterTopology from "models/database/cluster/clusterTopology";
import { useChanges } from "hooks/useChanges";
import { useAccessManager } from "hooks/useAccessManager";
import { NodeGroup } from "components/pages/resources/manageDatabaseGroup/NodeGroup";

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

    const [state, dispatch] = useReducer(manageDatabaseGroupReducer, null);
    const { databasesService } = useServices();
    const { status: licenseStatus } = useLicenseStatus();

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
        //TODO:if (!sortableMode) {
        fetchDatabaseInfo(db.name);
        //TODO:}
    }, [fetchDatabaseInfo, db]);

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

    /*
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
    
     */

    const settingsUniqueId = useId("settings");

    const changeDynamicDatabaseDistribution = useCallback(async () => {
        toggleDynamicDatabaseDistribution();

        await databasesService.toggleDynamicNodeAssignment(db, !dynamicDatabaseDistribution);
    }, [dynamicDatabaseDistribution, toggleDynamicDatabaseDistribution, databasesService, db]);

    if (!state) {
        return <Spinner />;
    }

    const dynamicDatabaseDistributionWarning = getDynamicDatabaseDistributionWarning(
        licenseStatus,
        state.encrypted,
        state.nodes.length
    );
    const enableDynamicDatabaseDistribution = isOperatorOrAbove() && !dynamicDatabaseDistributionWarning;

    return (
        <div className="content-margin">
            <div className="sticky-header">
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

            <NodeGroup
                nodes={state.nodes}
                db={db}
                deletionInProgress={state.deletionInProgress}
                refresh={refresh}
                lockMode={state.lockMode}
            />
        </div>
    );
}
