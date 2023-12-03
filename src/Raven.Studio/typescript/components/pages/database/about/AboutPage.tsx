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
import {
    Badge,
    Button,
    Card,
    CardBody,
    CardHeader,
    Col,
    Modal,
    ModalBody,
    ModalFooter,
    Row,
    TabContent,
    TabPane,
} from "reactstrap";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import "./AboutPage.scss";
import { act } from "react-dom/test-utils";
import classNames from "classnames";
import genUtils from "common/generalUtils";
import useBoolean from "components/hooks/useBoolean";

interface AboutPageProps {
    licenseType: Raven.Server.Commercial.LicenseType;
    status: Raven.Server.Commercial.Status;
    licenseExpiration?: string;
    licenseServerConnection: boolean;
    supportId?: string;
    newVersionAvailable?: string;
}

export function AboutPage(props: AboutPageProps) {
    const { licenseType, status, licenseExpiration, licenseServerConnection, supportId } = props;

    //External Links
    const ravendbHomeUrl = "https://ravendb.net";
    const githubDiscussionsUrl = "https://github.com/ravendb/ravendb/discussions";
    const facebookUrl = "https://github.com/ravendb/ravendb/discussions";
    const xUrl = "https://twitter.com/ravendb";
    const linkedinUrl = "https://www.linkedin.com/company/ravendb";
    const supportTermsUrl = "https://ravendb.net/terms";
    const updateInstructionsUrl = "#";

    //Image urls
    const LogoImg = require("Content/img/ravendb_logo.svg");
    const supportImg = require("Content/img/pages/about/support.svg");
    const supportCharacterImg = require("Content/img/pages/about/supportCharacter.svg");

    const [activeTab, setActiveTab] = useState("2");

    //Logic
    const getSupportType = (status: Raven.Server.Commercial.Status) => {
        switch (status) {
            case "ProfessionalSupport":
                return "Professional";
            case "ProductionSupport":
                return "Production";
            default:
                return "Community";
        }
    };

    const supportType = getSupportType(status);

    const PaidSupportAvailable = (supportType: string) => {
        return supportType === "Community";
    };

    const [newVersionAvailable, setNewVersionAvailable] = useState(null);

    const checkForUpdates = () => {
        setNewVersionAvailable("6.0.2 (60002) - 12.11.2023");
    };

    const handleTabClick = (tab: string) => {
        setActiveTab(tab);
        {
            console.log(activeTab);
        }
    };

    const { value: showChangelogModal, toggle: toggleShowChangelogModal } = useBoolean(false);

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
                                        <span className="text-cloud">{licenseType}</span>
                                    </OverallInfoItem>
                                    <OverallInfoItem icon="calendar" label="Expires">
                                        {genUtils.formatUtcDateAsLocal(licenseExpiration)}
                                        <div className="lh-sm small text-muted">
                                            ({genUtils.timeSpanAsAgo(licenseExpiration, false)})
                                        </div>
                                    </OverallInfoItem>
                                    <OverallInfoItem icon="raven" label="License server">
                                        {licenseServerConnection ? (
                                            <span className="text-success">
                                                <Icon icon="check" />
                                                Connected
                                            </span>
                                        ) : (
                                            <span className="text-warning">
                                                <Icon icon="warning" />
                                                Couldn't connect
                                            </span>
                                        )}
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
                                        {newVersionAvailable ? (
                                            <>
                                                <span className="text-warning">
                                                    <Icon icon="star-filled" />
                                                    Update Available
                                                </span>
                                                <div className="small lh-sm text-muted">{newVersionAvailable}</div>
                                                <a href={updateInstructionsUrl} className="small" target="_blank">
                                                    Update istructions <Icon icon="newtab" margin="ms-1" />
                                                </a>
                                            </>
                                        ) : (
                                            <span className="text-success">
                                                <Icon icon="check" />
                                                You are using the latest version
                                            </span>
                                        )}
                                    </OverallInfoItem>
                                    <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                                        <Button outline className="rounded-pill" onClick={toggleShowChangelogModal}>
                                            <Icon icon="logs" /> Changelog
                                        </Button>
                                        <Button outline className="rounded-pill" onClick={checkForUpdates}>
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
                                        <span className={classNames()}>{supportType}</span>
                                    </OverallInfoItem>
                                    {supportId && (
                                        <OverallInfoItem icon="user" label="Support ID">
                                            {supportId}
                                        </OverallInfoItem>
                                    )}

                                    <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                                        <Button className="rounded-pill" disabled={!PaidSupportAvailable}>
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
                        <Row className="about-page-tabs">
                            <Col>
                                <a
                                    className={classNames({ active: activeTab === "1" })}
                                    onClick={() => handleTabClick("1")}
                                >
                                    <Icon
                                        icon="license"
                                        className="circle-border fs-2"
                                        color={licenseServerConnection === true ? "success" : "warning"}
                                        margin="me-2"
                                    />
                                    <span className="fs-3">License details</span>
                                    <Badge className="rounded-pill py-1 ms-1 align-self-start" color="primary">
                                        1
                                    </Badge>
                                </a>
                            </Col>
                            <Col>
                                <a
                                    className={classNames({ active: activeTab === "2" })}
                                    onClick={() => handleTabClick("2")}
                                >
                                    <Icon icon="support" className="circle-border" margin="fs-2 me-2" />
                                    <span className="fs-3">Support plan</span>
                                    {!PaidSupportAvailable && (
                                        <Badge className="rounded-pill py-1 ms-1 align-self-start" color="primary">
                                            1
                                        </Badge>
                                    )}
                                </a>
                            </Col>
                        </Row>
                    </CardHeader>
                    <TabContent activeTab={activeTab}>
                        <TabPane tabId="1">
                            <h4>Tab 1 Contents</h4>
                        </TabPane>
                        <TabPane tabId="2">
                            <div className="bg-faded-info hstack justify-content-center">
                                <img src={supportCharacterImg} className="support-character-img mt-4" />
                            </div>
                            {supportType === "Community" && (
                                <>
                                    <RichPanelHeader className="text-center p-4">
                                        You are using
                                        <h2 className="text-info">Free Community Support</h2>
                                        <p>
                                            Get help and connect with fellow users and RavenDB developers through our
                                            community forum
                                        </p>
                                        <Button
                                            outline
                                            href={githubDiscussionsUrl}
                                            className="rounded-pill align-self-center px-3"
                                        >
                                            <Icon icon="group" /> Ask community <Icon icon="newtab" margin="ms-2" />
                                        </Button>
                                    </RichPanelHeader>
                                    <div className="text-center p-4 vstack align-items-center">
                                        <h2 className="hstack justify-content-center">
                                            <img src={supportImg} width={70} className="me-4" />
                                            Elevate your experience
                                        </h2>
                                        <div>
                                            <p className="max-paragraph-width">
                                                RavenDB Cloud Support has you covered. Whether it’s a simple question or
                                                a mission-critical emergency, our core developers will be on hand to
                                                provide expert assistance and advice around the clock.
                                            </p>
                                            <p>Here’s what you’ll get:</p>
                                        </div>
                                        <Row className="support-advantages">
                                            <SupportAdvantage icon="phone">Phone & Email support</SupportAdvantage>
                                            <SupportAdvantage icon="notifications">
                                                Request support directly from RavenDB Studio
                                            </SupportAdvantage>
                                            <SupportAdvantage icon="user">
                                                Access to RavenDB core developers
                                            </SupportAdvantage>
                                            <SupportAdvantage icon="clock">
                                                Up to 2 hour SLA
                                                <br />
                                                24/7 AVAILABILITY
                                            </SupportAdvantage>
                                        </Row>
                                        <Button color="success" className="px-4 rounded-pill mt-4" size="lg">
                                            <Icon icon="upgrade-arrow" /> <strong>Upgrade Your Support</strong>
                                        </Button>
                                    </div>
                                </>
                            )}
                            {supportType !== "Community" && (
                                <>
                                    <RichPanelHeader className="p-4">
                                        <Row>
                                            <Col xs={12} sm={6}>
                                                You are using
                                                <h2 className="text-info">{supportType} Support</h2>
                                            </Col>
                                            <Col xs={12} sm={6}></Col>
                                        </Row>

                                        <p>
                                            Get help and connect with fellow users and RavenDB developers through our
                                            community forum
                                        </p>
                                        <Button
                                            outline
                                            href={githubDiscussionsUrl}
                                            className="rounded-pill align-self-center px-3"
                                        >
                                            <Icon icon="group" /> Ask community <Icon icon="newtab" margin="ms-2" />
                                        </Button>
                                    </RichPanelHeader>
                                    <div className="text-center p-4 vstack align-items-center">
                                        <h2 className="hstack justify-content-center">
                                            <img src={supportImg} width={70} className="me-4" />
                                            Elevate your experience
                                        </h2>
                                        <div>
                                            <p className="max-paragraph-width">
                                                RavenDB Cloud Support has you covered. Whether it’s a simple question or
                                                a mission-critical emergency, our core developers will be on hand to
                                                provide expert assistance and advice around the clock.
                                            </p>
                                            <p>Here’s what you’ll get:</p>
                                        </div>
                                        <Row className="support-advantages">
                                            <SupportAdvantage icon="phone">Phone & Email support</SupportAdvantage>
                                            <SupportAdvantage icon="notifications">
                                                Request support directly from RavenDB Studio
                                            </SupportAdvantage>
                                            <SupportAdvantage icon="user">
                                                Access to RavenDB core developers
                                            </SupportAdvantage>
                                            <SupportAdvantage icon="clock">
                                                Up to 2 hour SLA
                                                <br />
                                                24/7 AVAILABILITY
                                            </SupportAdvantage>
                                        </Row>
                                        <Button color="success" className="px-4 rounded-pill mt-4" size="lg">
                                            <Icon icon="upgrade-arrow" /> <strong>Upgrade Your Support</strong>
                                        </Button>
                                    </div>
                                </>
                            )}
                        </TabPane>
                    </TabContent>
                </Card>
            </div>
            <Modal
                isOpen={showChangelogModal}
                toggle={toggleShowChangelogModal}
                wrapClassName="bs5"
                centered
                contentClassName={`modal-border bulge-warning`}
            >
                <ModalBody className="vstack gap-4 position-relative">
                    <div className="text-center">
                        <Icon icon="logs" color="warning" className="fs-1" margin="m-0" />
                    </div>

                    <div className="position-absolute m-2 end-0 top-0">
                        <Button close onClick={toggleShowChangelogModal} />
                    </div>
                    <div className="text-center lead">Changelog</div>
                </ModalBody>
                <ModalFooter>
                    <Button color="link" onClick={toggleShowChangelogModal} className="link-muted">
                        Cancel
                    </Button>
                </ModalFooter>
            </Modal>
        </>
    );
}

interface SupportAdvantageProps {
    icon: IconName;
    children: ReactNode | ReactNode[];
}

function SupportAdvantage(props: SupportAdvantageProps) {
    const { icon, children } = props;
    return (
        <Col>
            <Icon icon={icon} margin="m-0" className="fs-2" />
            <div className="small mt-1">
                <strong>{children}</strong>
            </div>
        </Col>
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
