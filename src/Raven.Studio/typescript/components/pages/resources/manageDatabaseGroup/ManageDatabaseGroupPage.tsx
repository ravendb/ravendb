import React, { useCallback } from "react";
import { Input, Label } from "reactstrap";
import { UncontrolledButtonWithDropdownPanel } from "components/common/DropdownPanel";
import useId from "hooks/useId";
import useBoolean from "hooks/useBoolean";
import { useServices } from "hooks/useServices";
import database from "models/resources/database";
import { useLicenseStatus } from "hooks/useLicenseStatus";
import LicenseStatus = Raven.Server.Commercial.LicenseStatus;
import clusterTopology from "models/database/cluster/clusterTopology";
import { useChanges } from "hooks/useChanges";
import { useAccessManager } from "hooks/useAccessManager";
import { NodeGroup } from "components/pages/resources/manageDatabaseGroup/NodeGroup";
import { useDatabaseManager } from "hooks/useDatabaseManager";

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

    const { databasesService } = useServices();
    const { status: licenseStatus } = useLicenseStatus();

    const { isOperatorOrAbove } = useAccessManager();

    const { databases, findByName } = useDatabaseManager();

    const {
        value: dynamicDatabaseDistribution,
        toggle: toggleDynamicDatabaseDistribution,
        setValue: setDynamicDatabaseDistribution,
    } = useBoolean(false); //tODO: assign default value

    const { serverNotifications } = useChanges();

    const dbShardedInfo = findByName(db.name);

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

    const dynamicDatabaseDistributionWarning = getDynamicDatabaseDistributionWarning(
        licenseStatus,
        dbShardedInfo.encrypted,
        dbShardedInfo.nodes.length
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
                nodes={dbShardedInfo.nodes}
                db={db}
                deletionInProgress={dbShardedInfo.deletionInProgress}
                refresh={() => {
                    //TODO:
                }}
                lockMode={dbShardedInfo.lockMode}
            />
        </div>
    );
}
