import React, { ReactNode, useEffect } from "react";
import { useAppUrls } from "hooks/useAppUrls";
import {
    initView,
    statisticsViewSelectors,
} from "components/pages/database/status/statistics/store/statisticsViewSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { IndexesDatabaseStats } from "components/pages/database/status/statistics/partials/IndexesDatabaseStats";
import { StatsHeader } from "components/pages/database/status/statistics/partials/StatsHeader";
import { NonShardedViewProps } from "components/models/common";
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
import { Button, Card, CardBody, Col, Row } from "reactstrap";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";

interface AboutPageProps {
    test?: boolean;
}

const LogoImg = require("Content/img/ravendb_logo.svg");

export function AboutPage(props: AboutPageProps) {
    const { test } = props;
    return (
        <>
            <div className="hstack flex-wrap gap-5 flex-grow-1">
                <div className="flex-grow vstack gap-4 align-items-center" style={{ maxWidth: "800px" }}>
                    <img src={LogoImg} width="200" />
                    <div className="vstack gap-3">
                        <Card>
                            <CardBody>
                                <h4>License</h4>
                                <Row>
                                    <OverallInfoItem icon="license" label="License type">
                                        <span className="text-cloud">Community (Cloud)</span>
                                    </OverallInfoItem>
                                    <OverallInfoItem icon="calendar" label="Expires">
                                        2024 January 26th
                                        <br />
                                        <small>(in 2 months 30 days)</small>
                                    </OverallInfoItem>
                                    <OverallInfoItem icon="raven" label="License server">
                                        <span className="text-success">
                                            <Icon icon="check" />
                                            Connected
                                        </span>
                                    </OverallInfoItem>
                                    <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                                        <Button outline className="rounded-pill">
                                            <Icon icon="replace" /> Replace
                                        </Button>
                                        <Button outline className="rounded-pill">
                                            <Icon icon="force" /> Force Update
                                            <Icon icon="info" color="info" margin="ms-1" />
                                        </Button>
                                    </Col>
                                </Row>
                            </CardBody>
                        </Card>
                        <Card>
                            <CardBody>
                                <h4>Software Version</h4>
                                <Row>
                                    <OverallInfoItem icon="server" label="Server version">
                                        6.0.1-nightly-20231011-1134
                                    </OverallInfoItem>
                                    <OverallInfoItem icon="client" label="Studio version">
                                        6.0.1-nightly-20231011-1134
                                    </OverallInfoItem>
                                    <OverallInfoItem icon="global" label="Updates">
                                        <span className="text-success">
                                            <Icon icon="check" />
                                            You are using the latest version
                                        </span>
                                    </OverallInfoItem>
                                    <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                                        <Button outline className="rounded-pill">
                                            <Icon icon="logs" /> Changelog
                                        </Button>
                                        <Button outline className="rounded-pill">
                                            <Icon icon="refresh" /> Check for updates
                                        </Button>
                                    </Col>
                                </Row>
                            </CardBody>
                        </Card>
                        <Card>
                            <CardBody>
                                <h4>Support</h4>
                                <Row>
                                    <OverallInfoItem icon="support" label="Support type">
                                        Community
                                    </OverallInfoItem>

                                    <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                                        <Button className="rounded-pill">
                                            <Icon icon="notifications" /> Request support
                                        </Button>
                                        <Button outline className="rounded-pill">
                                            <Icon icon="group" /> Ask community <Icon icon="newtab" margin="ms-1" />
                                        </Button>
                                    </Col>
                                </Row>
                            </CardBody>
                        </Card>
                    </div>
                    <div className="hstack">
                        <Button color="info" className="d-flex rounded-pill align-items-center py-1 ps-3 pe-4">
                            <Icon icon="rocket" margin="me-2" className="fs-2"></Icon>
                            <div className="text-start lh-1">
                                <div className="small">Help us improve</div>
                                <strong>Send Feedback</strong>
                            </div>
                        </Button>
                        <Button color="link" href="#">
                            <Icon icon="facebook" />
                        </Button>
                    </div>
                </div>
                <Card className="flex-grow">
                    <RichPanelHeader>Test</RichPanelHeader>
                </Card>
            </div>
        </>
    );
}

interface OverallInfoItemProps {
    icon: IconName;
    label: string;
    children: string | ReactNode | ReactNode[];
}

function OverallInfoItem(props: OverallInfoItemProps) {
    const { icon, label, children } = props;
    return (
        <Col sm={6} className="mb-3">
            <div className="d-flex">
                <Icon icon={icon} className="fs-1" margin="me-3 mt-2" />
                <div className="vstack">
                    <small className="text-muted">{label}</small>
                    <strong className="fs-4 text-emphasis">{children}</strong>
                </div>
            </div>
        </Col>
    );
}
