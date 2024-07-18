import React, { useCallback } from "react";
import { Button, Input, Label } from "reactstrap";
import { UncontrolledButtonWithDropdownPanel } from "components/common/DropdownPanel";
import useId from "hooks/useId";
import useBoolean from "hooks/useBoolean";
import { useServices } from "hooks/useServices";
import { NodeGroup } from "components/pages/resources/manageDatabaseGroup/partials/NodeGroup";
import { OrchestratorsGroup } from "components/pages/resources/manageDatabaseGroup/partials/OrchestratorsGroup";
import { ShardsGroup } from "components/pages/resources/manageDatabaseGroup/partials/ShardsGroup";
import { FlexGrow } from "components/common/FlexGrow";
import app from "durandal/app";
import addNewShardToDatabaseGroup from "viewmodels/resources/addNewShardToDatabaseGroup";
import { StickyHeader } from "components/common/StickyHeader";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { SortableModeCounterProvider } from "./partials/useSortableModeCounter";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";

function getDynamicDatabaseDistributionWarning(
    hasDynamicNodesDistribution: boolean,
    encryptedDatabase: boolean,
    nodesCount: number
) {
    if (!hasDynamicNodesDistribution) {
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

export function ManageDatabaseGroupPage() {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const hasDynamicNodesDistribution = useAppSelector(licenseSelectors.statusValue("HasDynamicNodesDistribution"));

    const { databasesService } = useServices();

    const { value: dynamicDatabaseDistribution, toggle: toggleDynamicDatabaseDistribution } = useBoolean(
        db.isDynamicNodesDistribution
    );

    const settingsUniqueId = useId("settings");

    const addNewShard = useCallback(() => {
        const addShardView = new addNewShardToDatabaseGroup(db.name);
        app.showBootstrapDialog(addShardView);
    }, [db.name]);

    const changeDynamicDatabaseDistribution = useCallback(async () => {
        toggleDynamicDatabaseDistribution();

        await databasesService.toggleDynamicNodeAssignment(db.name, !dynamicDatabaseDistribution);
    }, [dynamicDatabaseDistribution, toggleDynamicDatabaseDistribution, databasesService, db.name]);

    const dynamicDatabaseDistributionWarning = getDynamicDatabaseDistributionWarning(
        hasDynamicNodesDistribution,
        db.isEncrypted,
        db.nodes.length
    );
    const enableDynamicDatabaseDistribution = isOperatorOrAbove && !dynamicDatabaseDistributionWarning;

    return (
        <div className="content-margin">
            <StickyHeader>
                <div className="flex-horizontal">
                    {!db.isSharded && (
                        <UncontrolledButtonWithDropdownPanel buttonText="Settings">
                            <>
                                <Label className="dropdown-item-text m-0" htmlFor={settingsUniqueId}>
                                    <div className="d-flex gap-3 form-switch">
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
                                    <div className="bg-faded-warning px-4 py-2">
                                        {dynamicDatabaseDistributionWarning}
                                    </div>
                                )}
                            </>
                        </UncontrolledButtonWithDropdownPanel>
                    )}

                    <FlexGrow />
                    {db.isSharded && (
                        <Button color="shard" onClick={addNewShard}>
                            <Icon icon="shard" addon="plus" />
                            Add Shard
                        </Button>
                    )}
                </div>
            </StickyHeader>
            <SortableModeCounterProvider>
                {db.isSharded ? (
                    <React.Fragment key="sharded-db">
                        <OrchestratorsGroup />
                        {db.shards.map((shard) => {
                            return <ShardsGroup key={shard.name} db={shard} />;
                        })}
                    </React.Fragment>
                ) : (
                    <NodeGroup key="non-sharded-db" db={db} />
                )}
            </SortableModeCounterProvider>
        </div>
    );
}
