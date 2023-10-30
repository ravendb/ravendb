import React from "react";
import { Button, Card, CardBody, Col, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { todo } from "common/developmentHelper";
import { useAppSelector } from "components/store";
import { NonShardedViewProps } from "components/models/common";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { HrHeader } from "components/common/HrHeader";
import ConflictResolutionConfigPanel from "components/pages/database/settings/conflictResolution/ConflictResolutionConfigPanel";
import { Switch } from "components/common/Checkbox";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

todo("Feature", "Damian", "Add missing logic");
todo("Feature", "Damian", "Connect to Studio");
todo("Feature", "Damian", "Remove legacy code");
todo("Other", "Danielle", "Add Info Hub text");

export default function ConflictResolution({ db }: NonShardedViewProps) {
    const conflictResolutionDocsLink = useRavenLink({ hash: "QRCNKH" });

    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    return (
        <>
            <div className="content-margin">
                <Col xxl={12}>
                    <Row className="gy-sm">
                        <Col>
                            <AboutViewHeading title="Conflict Resolution" icon="conflicts-resolution" />
                            {isDatabaseAdmin && (
                                <div id="newConflictResolutionScript" className="d-flex w-fit-content gap-3 mb-3">
                                    <ButtonWithSpinner color="primary" icon="save" isSpinning={null} disabled>
                                        Save
                                    </ButtonWithSpinner>
                                </div>
                            )}
                            <div className="mb-3">
                                <HrHeader
                                    right={
                                        isDatabaseAdmin && (
                                            <div id="addNewScriptButton">
                                                <Button
                                                    color="info"
                                                    size="sm"
                                                    className="rounded-pill"
                                                    title="Add a new Conflicts Resolution script"
                                                >
                                                    <Icon icon="plus" />
                                                    Add new
                                                </Button>
                                            </div>
                                        )
                                    }
                                    count={1}
                                >
                                    <Icon icon="documents" />
                                    Collection-specific scripts
                                </HrHeader>
                                <ConflictResolutionConfigPanel isDatabaseAdmin={isDatabaseAdmin} />
                            </div>
                            {isDatabaseAdmin && (
                                <Card>
                                    <CardBody>
                                        <Switch color="primary" selected={null} toggleSelection={null}>
                                            If no script was defined for a collection, resolve the conflict using the
                                            latest version
                                        </Switch>
                                    </CardBody>
                                </Card>
                            )}
                        </Col>
                        <Col sm={12} lg={4}>
                            <AboutViewAnchored>
                                <AccordionItemWrapper
                                    targetId="1"
                                    icon="about"
                                    color="info"
                                    description="Get additional info on this feature"
                                    heading="About this view"
                                >
                                    <p>Text for Conflicts Resolution</p>
                                    <hr />
                                    <div className="small-label mb-2">useful links</div>
                                    <a href={conflictResolutionDocsLink} target="_blank">
                                        <Icon icon="newtab" /> Docs - Conflict Resolution
                                    </a>
                                </AccordionItemWrapper>
                            </AboutViewAnchored>
                        </Col>
                    </Row>
                </Col>
            </div>
        </>
    );
}
