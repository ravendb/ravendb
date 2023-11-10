import React, { useState } from "react";
import { Button, Col, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { todo } from "common/developmentHelper";
import { useAppSelector } from "components/store";
import { NonShardedViewProps } from "components/models/common";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { HrHeader } from "components/common/HrHeader";
import ConnectionStringsConfigPanel from "components/pages/database/settings/connectionStrings/ConnectionStringsConfigPanel";
import { ConnectionStringsInfoHub } from "viewmodels/database/settings/ConnectionStringsInfoHub";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";

todo("Feature", "Damian", "Add missing logic");
todo("Feature", "Damian", "Connect to Studio");
todo("Feature", "Damian", "Remove legacy code");

export default function ConnectionStrings({ db }: NonShardedViewProps) {
    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    const [isEditModalOpen, setEditModalOpen] = useState(false);

    const toggleEditModalOpen = () => {
        setEditModalOpen(!isEditModalOpen);
    };

    return (
        <>
            <div className="content-margin">
                <Col xxl={12}>
                    <Row className="gy-sm">
                        <Col>
                            <AboutViewHeading title="Connection Strings" icon="manage-connection-strings" />
                            <Button color="primary" onClick={() => setEditModalOpen(true)}>
                                <Icon icon="plus" />
                                Add new
                            </Button>
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
                                                    onClick={() => setEditModalOpen(true)}
                                                >
                                                    <Icon icon="plus" />
                                                    Add new
                                                </Button>
                                                {isEditModalOpen && (
                                                    <EditConnectionStrings
                                                        isOpen={isEditModalOpen}
                                                        toggle={toggleEditModalOpen}
                                                    />
                                                )}
                                            </div>
                                        )
                                    }
                                >
                                    RavenDB
                                </HrHeader>
                                <ConnectionStringsConfigPanel isDatabaseAdmin={isDatabaseAdmin} />
                            </div>
                        </Col>
                        <Col sm={12} lg={4}>
                            <ConnectionStringsInfoHub />
                        </Col>
                    </Row>
                </Col>
            </div>
        </>
    );
}
