import React from "react";
import { Col, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { todo } from "common/developmentHelper";
import { useAppSelector } from "components/store";
import { HrHeader } from "components/common/HrHeader";
import { IntegrationsInfoHub } from "viewmodels/database/settings/IntegrationsInfoHub";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import IntegrationsUserList from "components/pages/database/settings/integrations/IntegrationsUserList";
import { useIntegrations } from "components/pages/database/settings/integrations/useIntegrations";
import IntegrationsAlerts from "components/pages/database/settings/integrations/IntegrationsAlerts";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import DatabaseUtils from "components/utils/DatabaseUtils";
import IntegrationsAddNewButton from "components/pages/database/settings/integrations/IntegrationsAddNewButton";

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

    const db = useAppSelector(databaseSelectors.activeDatabase);

    if (db.isSharded || DatabaseUtils.isSharded(db.name)) {
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
        <Row className="gy-sm content-margin">
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
    );
}
