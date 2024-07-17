import React from "react";
import { Col, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import IntegrationsUserList from "components/pages/database/settings/integrations/IntegrationsUserList";
import { useIntegrations } from "components/pages/database/settings/integrations/useIntegrations";
import IntegrationsAlerts from "components/pages/database/settings/integrations/IntegrationsAlerts";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import IntegrationsAddNewButton from "components/pages/database/settings/integrations/IntegrationsAddNewButton";
import { IntegrationsInfoHub } from "components/pages/database/settings/integrations/IntegrationsInfoHub";

export default function Integrations() {
    const {
        isSharded,
        isLicenseUpgradeRequired,
        isPostgreSqlSupportEnabled,
        asyncGetPostgreSqlUsers,
        users,
        addNewUser,
        removeUser,
    } = useIntegrations();

    if (isSharded) {
        return (
            <FeatureNotAvailable>
                <span>
                    Integrations are not available for <Icon icon="sharding" color="shard" margin="m-0" /> sharded
                    databases
                </span>
            </FeatureNotAvailable>
        );
    }

    return (
        <div className="content-margin">
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading title="Integrations" icon="integrations" />
                    <div className="mb-3">
                        <HrHeader
                            right={
                                <IntegrationsAddNewButton
                                    isLicenseUpgradeRequired={isLicenseUpgradeRequired}
                                    addNewUser={addNewUser}
                                />
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
        </div>
    );
}
