import React, { ReactNode, useEffect, useState } from "react";
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
import { Button, Card, CardBody, CardHeader, Col, Nav, NavItem, NavLink, Row, TabContent, TabPane } from "reactstrap";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import "./AboutPage.scss";
import { act } from "react-dom/test-utils";

interface AboutPageProps {
    test?: boolean;
}

export function AboutPage(props: AboutPageProps) {
    const { test } = props;

    //External Links
    const ravendbHomeUrl = "https://ravendb.net";
    const githubDiscussionsUrl = "https://github.com/ravendb/ravendb/discussions";
    const facebookUrl = "https://github.com/ravendb/ravendb/discussions";
    const xUrl = "https://twitter.com/ravendb";
    const linkedinUrl = "https://www.linkedin.com/company/ravendb";

    //Image urls
    const LogoImg = require("Content/img/ravendb_logo.svg");
    const supportImg = require("Content/img/pages/about/support.svg");
    const supportCharacterImg = require("Content/img/pages/about/supportCharacter.svg");

    const [activeTab, setActiveTab] = useState("2");

    const handleTabClick = (tab: string) => {
        setActiveTab(tab);
        {
            console.log(activeTab);
        }
    };

    return (
        <>
            <div className="hstack flex-wrap gap-5 flex-grow-1 align-items-stretch justify-content-center">
                <div className="vstack gap-4 align-items-center" style={{ maxWidth: "800px" }}>
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
                    <div>
                        <div className="hstack justify-content-center">
                            <Button color="info" className="d-flex rounded-pill align-items-center py-1 ps-3 pe-4">
                                <Icon icon="rocket" margin="me-2" className="fs-2"></Icon>
                                <div className="text-start lh-1">
                                    <div className="small">Help us improve</div>
                                    <strong>Send Feedback</strong>
                                </div>
                            </Button>
                            <div className="d-flex align-item text-center ms-4">
                                <a href={ravendbHomeUrl} className="text-emphasis p-2" target="_blank">
                                    <Icon icon="global" margin="m-0" />
                                </a>
                                <a href={facebookUrl} className="text-emphasis p-2" target="_blank">
                                    <Icon icon="facebook" margin="m-0" />
                                </a>
                                <a href={xUrl} className="text-emphasis p-2" target="_blank">
                                    <Icon icon="twitter" margin="m-0" />
                                </a>
                                <a href={linkedinUrl} className="text-emphasis p-2" target="_blank">
                                    <Icon icon="linkedin" margin="m-0" />
                                </a>
                            </div>
                        </div>
                        <div className="small text-muted mt-3 text-center">
                            Copyright © 2009 - 2023 Hibernating Rhinos. All rights reserved.
                        </div>
                    </div>
                </div>
                <Card className="flex-grow">
                    <CardHeader>
                        <Nav justified pills>
                            <NavItem>
                                <NavLink className={activeTab === "1" && "active"} onClick={() => handleTabClick("1")}>
                                    <Icon icon="license" /> License details
                                </NavLink>
                            </NavItem>
                            <NavItem>
                                <NavLink className={activeTab === "2" && "active"} onClick={() => handleTabClick("2")}>
                                    <Icon icon="support" /> Support plan
                                </NavLink>
                            </NavItem>
                        </Nav>
                    </CardHeader>
                    <TabContent activeTab={activeTab}>
                        <TabPane tabId="1">
                            <h4>Tab 1 Contents</h4>
                        </TabPane>
                        <TabPane tabId="2">
                            <div className="bg-faded-info hstack justify-content-center">
                                <img src={supportCharacterImg} className="support-character-img mt-4" />
                            </div>
                            <RichPanelHeader className="text-center p-4">
                                You are using
                                <h2 className="text-info">Free Community Support</h2>
                                <p>
                                    Get help and connect with fellow users and RavenDB developers through our community
                                    forum
                                </p>
                                <Button
                                    outline
                                    href={githubDiscussionsUrl}
                                    className="rounded-pill align-self-center px-3"
                                >
                                    <Icon icon="group" /> Ask community <Icon icon="newtab" margin="ms-2" />
                                </Button>
                            </RichPanelHeader>
                            <div className="text-center p-4">
                                <h2 className="hstack justify-content-center">
                                    <img src={supportImg} width={70} className="me-4" />
                                    Elevate your experience
                                </h2>
                                <p>
                                    RavenDB Cloud Support has you covered. Whether it’s a simple question or a
                                    mission-critical emergency, our core developers will be on hand to provide expert
                                    assistance and advice around the clock.
                                </p>
                                <p>Here’s what you’ll get:</p>
                            </div>
                        </TabPane>
                    </TabContent>
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
