import React, { useState } from "react";
import { Alert, Button, Col, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { todo } from "common/developmentHelper";
import { useAppSelector } from "components/store";
import { NonShardedViewProps } from "components/models/common";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { HrHeader } from "components/common/HrHeader";
import IntegrationsConfigPanel from "components/pages/database/settings/integrations/IntegrationsConfigPanel";
import { IntegrationsInfoHub } from "viewmodels/database/settings/IntegrationsInfoHub";

todo("Feature", "Damian", "Add missing logic");
todo("Feature", "Damian", "Connect to Studio");
todo("Feature", "Damian", "Remove legacy code");

export default function Integrations({ db }: NonShardedViewProps) {
    const postgreSqlDocsLink = useRavenLink({ hash: "HDTCH7" });

    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    const [isPostgreSqlSupportEnabled] = useState(false);

    const [configPanels, setConfigPanels] = useState([{ panelCollapsed: true }]);

    const addNewPanel = () => {
        setConfigPanels([...configPanels, { panelCollapsed: false }]);
    };

    return (
        <>
            <div className="content-margin">
                <Col xxl={12}>
                    <Row className="gy-sm">
                        <Col>
                            <AboutViewHeading title="Integrations" icon="integrations" />
                            <div className="mb-3">
                                <HrHeader
                                    right={
                                        isDatabaseAdmin && (
                                            <div id="addNewCredentialsButton">
                                                <Button
                                                    color="info"
                                                    size="sm"
                                                    className="rounded-pill"
                                                    title="Add new credentials"
                                                    onClick={addNewPanel}
                                                >
                                                    <Icon icon="plus" />
                                                    Add new
                                                </Button>
                                            </div>
                                        )
                                    }
                                >
                                    PostgreSQL Protocol Credentials
                                </HrHeader>
                                {configPanels.map((panel, index) => (
                                    <IntegrationsConfigPanel
                                        key={index}
                                        isDatabaseAdmin={isDatabaseAdmin}
                                        panelCollapsed={panel.panelCollapsed}
                                    />
                                ))}
                            </div>
                            {!isPostgreSqlSupportEnabled && (
                                <Alert color="warning" className="mt-3">
                                    PostgreSQL support must be explicitly enabled in your <code>settings.json</code>{" "}
                                    file. Learn more <a href={postgreSqlDocsLink}>here</a>.
                                </Alert>
                            )}
                        </Col>
                        <Col sm={12} lg={4}>
                            <IntegrationsInfoHub />
                        </Col>
                    </Row>
                </Col>
            </div>
        </>
    );
}
