import React from "react";
import { Button, Col, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { todo } from "common/developmentHelper";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { HrHeader } from "components/common/HrHeader";
import { IntegrationsInfoHub } from "viewmodels/database/settings/IntegrationsInfoHub";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import IntegrationsUserList from "components/pages/database/settings/integrations/IntegrationsUserList";
import { useIntegrations } from "components/pages/database/settings/integrations/useIntegrations";
import IntegrationsAlerts from "components/pages/database/settings/integrations/IntegrationsAlerts";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";

todo("Feature", "Damian", "Connect to Studio");
todo("Feature", "Damian", "Remove legacy code");

export default function Integrations() {
    const {
        isLicenseUpgradeRequired,
        isPostgreSqlSupportEnabled,
        asyncGetPostgreSqlUsers,
        users,
        addNewUser,
        removeUser,
    } = useIntegrations();

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.hasDatabaseAdminAccess());
    const isSharded = useAppSelector(databaseSelectors.activeDatabase).isSharded;

    // TODO or is view for one shard?
    if (isSharded) {
        return (
            <FeatureNotAvailable>
                Integrations are not available for <Icon icon="sharding" color="shard" margin="m-0" /> sharded databases
            </FeatureNotAvailable>
        );
    }

    return (
        <Row className="gy-sm content-margin">
            <Col>
                <AboutViewHeading title="Integrations" icon="integrations" />
                <div className="mb-3">
                    <HrHeader
                        right={
                            hasDatabaseAdminAccess && (
                                <Button
                                    color="info"
                                    size="sm"
                                    className="rounded-pill"
                                    title="Add new credentials"
                                    onClick={addNewUser}
                                >
                                    <Icon icon="plus" />
                                    Add new
                                </Button>
                            )
                        }
                    >
                        PostgreSQL Protocol Credentials
                    </HrHeader>
                    <IntegrationsUserList
                        fetchState={asyncGetPostgreSqlUsers.status}
                        reload={asyncGetPostgreSqlUsers.execute}
                        users={users}
                        removeUser={removeUser}
                    />
                </div>
                <IntegrationsAlerts
                    isLicenseUpgradeRequired={isLicenseUpgradeRequired}
                    isPostgreSqlSupportEnabled={isPostgreSqlSupportEnabled}
                />
            </Col>
            <Col sm={12} lg={4}>
                <IntegrationsInfoHub />
            </Col>
        </Row>
    );
}
