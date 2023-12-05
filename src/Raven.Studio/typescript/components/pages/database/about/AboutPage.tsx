import React, { ChangeEvent, ReactNode, useState } from "react";
import { RichPanelHeader } from "components/common/RichPanel";
import {
    Badge,
    Button,
    Card,
    CardBody,
    CardHeader,
    Col,
    Collapse,
    Form,
    FormGroup,
    Input,
    Label,
    Modal,
    ModalBody,
    ModalFooter,
    Row,
    TabContent,
    TabPane,
    UncontrolledPopover,
} from "reactstrap";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import "./AboutPage.scss";

import classNames from "classnames";
import genUtils from "common/generalUtils";
import useBoolean from "components/hooks/useBoolean";
import { FlexGrow } from "components/common/FlexGrow";
import { Checkbox, Switch } from "components/common/Checkbox";

interface AboutPageProps {
    licenseType: Raven.Server.Commercial.LicenseType;
    isCloud: boolean;
    isEnabled: boolean;
    isIsv: boolean;
    status: Raven.Server.Commercial.Status;
    licenseExpiration?: string;
    licenseServerConnection: boolean;
    newVersionAvailable?: string;
}

export function AboutPage(props: AboutPageProps) {
    const { licenseType, isCloud, isEnabled, status, licenseExpiration, licenseServerConnection } = props;

    //External Links
    const ravendbHomeUrl = "https://ravendb.net";
    const githubDiscussionsUrl = "https://github.com/ravendb/ravendb/discussions";
    const facebookUrl = "https://github.com/ravendb/ravendb/discussions";
    const xUrl = "https://twitter.com/ravendb";
    const linkedinUrl = "https://www.linkedin.com/company/ravendb";
    const supportTermsUrl = "https://ravendb.net/terms";
    const getLicenseUrl = "https://ravendb.net/buy";
    const upgradeSupportUrl = "#";
    const updateInstructionsUrl = "#";
    const changelogDocsUrl = "#";

    //Image urls
    const LogoImg = require("Content/img/ravendb_logo.svg");
    const supportImg = require("Content/img/pages/about/support.svg");
    const supportCharacterImg = require("Content/img/pages/about/supportCharacter.svg");

    const [activeTab, setActiveTab] = useState("license");

    const supportId = "5234067";
    const licenseId = "63f96f9e-e2fd-40af-9732-131c8471f68e";
    const licenseTo = "Hibernating Rhinos - Production";

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

    const PaidSupportAvailable = supportType !== "Community";

    const [newVersionAvailable, setNewVersionAvailable] = useState(null);

    const checkForUpdates = () => {
        setNewVersionAvailable("6.0.2 (60002) - 12.11.2023");
    };

    const handleTabClick = (tab: string) => {
        setActiveTab(tab);
    };

    const { value: showChangelogModal, toggle: toggleShowChangelogModal } = useBoolean(false);
    const { value: showRequestSupportModal, toggle: toggleShowRequestSupportModal } = useBoolean(false);
    const { value: showReplaceLicenseModal, toggle: toggleShowReplaceLicenseModal } = useBoolean(false);

    const requestSupportBtnHandler = () => {
        if (PaidSupportAvailable) {
            handleTabClick("support");
            toggleShowRequestSupportModal();
        } else {
            handleTabClick("support");
        }
    };

    const [searchText, setSearchText] = useState("");

    const onSearchTextChange = (searchText: string) => {
        setSearchText(searchText);
    };

    const featureAvailabilityData: FeatureAvailabilityData[] = [
        {
            featureName: "Default Policy",

            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
        },
        {
            featureName: "Max revisions",

            community: { value: 2 },
            professional: { value: Infinity },
            enterprise: { value: Infinity },
        },
        // {
        //     featureName: "Max revision days",

        //     community: { value: isCloud ? 38 : 45 },
        //     professional: { value: Infinity },
        //     enterprise: { value: Infinity },
        // },
    ];

    return (
        <>
            <div className="hstack flex-wrap gap-5 flex-grow-1 align-items-stretch justify-content-evenly about-page">
                <div className="vstack gap-4 align-items-center" style={{ maxWidth: "800px" }}>
                    <img src={LogoImg} width="200" />
                    <div className="vstack gap-3">
                        {!isEnabled && (
                            <Card color="faded-primary">
                                <CardBody className="text-body">
                                    <h3>
                                        <Icon icon="info" />
                                        The running server is in a <span className="fw-bolder">Passive State</span>, it
                                        is not part of a cluster yet.
                                    </h3>
                                    <p>
                                        Your license information will be visible only when the server is part of a
                                        cluster.
                                    </p>
                                    <p>Either one of the following can be done to Bootstrap a Cluster:</p>
                                    <ul>
                                        <li>
                                            Create a <a href="#">New database</a>
                                        </li>
                                        <li>
                                            <a href="#">Register a license</a> (if not registered yet)
                                        </li>
                                        <li>
                                            Bootstrap the cluster on the <a href="#">Cluster View</a>
                                            <br />
                                            (or add another node, resulting in both nodes being part of the cluster)
                                        </li>
                                    </ul>
                                </CardBody>
                            </Card>
                        )}
                        <Card>
                            <CardBody>
                                <h4>License</h4>
                                <Row>
                                    <OverallInfoItem icon="license" label="License type">
                                        <span className={classNames({ "text-cloud": isCloud })}>
                                            {licenseType} {isCloud && <>(Cloud)</>}
                                        </span>
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
                                                Couldn&apos;t connect
                                            </span>
                                        )}
                                    </OverallInfoItem>
                                    <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                                        <Button
                                            outline
                                            className="rounded-pill"
                                            onClick={toggleShowReplaceLicenseModal}
                                        >
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
                                        <span
                                            className={classNames(
                                                { "text-professional": supportType === "Professional" },
                                                { "text-enterprise": supportType === "Production" }
                                            )}
                                        >
                                            {supportType}
                                        </span>
                                    </OverallInfoItem>
                                    {supportId && (
                                        <OverallInfoItem icon="user" label="Support ID">
                                            {supportId}
                                        </OverallInfoItem>
                                    )}

                                    <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                                        <Button
                                            className="rounded-pill"
                                            color={PaidSupportAvailable ? "primary" : "secondary"}
                                            onClick={() => requestSupportBtnHandler()}
                                        >
                                            <Icon icon="notifications" /> Request support
                                        </Button>
                                        <Button
                                            outline
                                            className="rounded-pill"
                                            href={githubDiscussionsUrl}
                                            target="_blank"
                                        >
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
                <Card className="flex-grow" style={{ maxWidth: "1100px" }}>
                    <CardHeader>
                        <Row className="about-page-tabs">
                            <Col>
                                <a
                                    className={classNames({ active: activeTab === "license" })}
                                    onClick={() => handleTabClick("license")}
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
                                    className={classNames({ active: activeTab === "support" })}
                                    onClick={() => handleTabClick("support")}
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
                        <TabPane tabId="license">
                            {licenseType !== "None" ? (
                                <Row className="text-center pt-4">
                                    <Col>
                                        <div className="small text-muted">license ID</div>
                                        <h4 className="fw-bolder text-emphasis m-0">{licenseId}</h4>
                                    </Col>
                                    <Col>
                                        <div className="small text-muted">license To</div>
                                        <h4 className="fw-bolder text-emphasis m-0">{licenseTo}</h4>
                                    </Col>
                                </Row>
                            ) : (
                                <div className="text-center pt-4 px-4">
                                    <h3 className="fw-bolder text-warning d-flex align-items-center justify-content-center">
                                        <Icon icon="empty-set" className="fs-1" margin="me-3" /> No license - AGPLv3
                                        Restrictions Applied
                                    </h3>
                                    <Button
                                        color="success"
                                        className="px-4 rounded-pill"
                                        size="lg"
                                        href={getLicenseUrl}
                                        target="_blank"
                                    >
                                        <strong>Get free license</strong>
                                        <Icon icon="newtab" margin="ms-2" />
                                    </Button>
                                </div>
                            )}

                            <hr />
                            <div className="px-4">
                                <div className="clearable-input">
                                    <Input
                                        type="text"
                                        accessKey="/"
                                        placeholder="Filter: e.g. ETL"
                                        title="Filter indexes"
                                        className="filtering-input"
                                        value={searchText}
                                        onChange={(e) => onSearchTextChange(e.target.value)}
                                    />
                                    {searchText && (
                                        <div className="clear-button">
                                            <Button color="secondary" size="sm" onClick={() => onSearchTextChange("")}>
                                                <Icon icon="clear" margin="m-0" />
                                            </Button>
                                        </div>
                                    )}
                                </div>
                            </div>
                            <Row></Row>
                        </TabPane>
                        <TabPane tabId="support">
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
                                            target="_blank"
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
                                        <Button
                                            color="success"
                                            className="px-4 rounded-pill mt-4"
                                            size="lg"
                                            href={upgradeSupportUrl}
                                            target="_blank"
                                        >
                                            <Icon icon="upgrade-arrow" /> <strong>Upgrade Your Support</strong>
                                        </Button>
                                    </div>
                                </>
                            )}
                            {supportType !== "Community" && (
                                <>
                                    <RichPanelHeader className="px-4 py-5 vstack gap-4">
                                        <Row className="g-md">
                                            <Col xs={12} sm={6} className="text-end">
                                                <div className="d-flex align-items-center">
                                                    <div className="flex-grow">
                                                        You are using
                                                        <h2
                                                            className={classNames(
                                                                "text-info m-0",
                                                                { "text-professional": supportType === "Professional" },
                                                                { "text-enterprise": supportType === "Production" }
                                                            )}
                                                        >
                                                            {supportType} Support
                                                        </h2>
                                                    </div>
                                                    <img src={supportImg} width={70} className="ms-4" />
                                                </div>
                                            </Col>
                                            <Col xs={12} sm={6}>
                                                <div className="vstack gap-4">
                                                    <div className="hstack">
                                                        <Icon icon="user" className="fs-2" />
                                                        <div className="small">
                                                            <strong>Support ID</strong>
                                                            <div>{supportId}</div>
                                                        </div>
                                                    </div>

                                                    <div className="hstack">
                                                        <Icon icon="clock" className="fs-2" />

                                                        <div className="small">
                                                            {supportType === "Professional" && (
                                                                <>
                                                                    <strong>Next business day SLA</strong>
                                                                    <div>24/7 AVAILABILITY</div>
                                                                </>
                                                            )}
                                                            {supportType === "Production" && (
                                                                <>
                                                                    <strong>2 hour SLA</strong>
                                                                    <div>24/7 AVAILABILITY</div>
                                                                </>
                                                            )}
                                                        </div>
                                                    </div>
                                                    <div>
                                                        <a href={supportTermsUrl} className="d-inline-flex">
                                                            <Icon icon="terms" className="fs-2" />
                                                            <div className="small">Terms and conditions</div>
                                                        </a>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                        <Row className="g-md">
                                            <Col className="text-center">
                                                Get help and connect with fellow users and RavenDB developers through
                                                our community forum
                                            </Col>
                                            <Col>
                                                <Button
                                                    outline
                                                    href={githubDiscussionsUrl}
                                                    className="rounded-pill align-self-center px-3"
                                                    target="_blank"
                                                >
                                                    <Icon icon="group" /> Ask community{" "}
                                                    <Icon icon="newtab" margin="ms-2" />
                                                </Button>
                                            </Col>
                                        </Row>
                                        <Row className="g-md">
                                            <Col className="text-center">
                                                Message support directly, with access to RavenDB core developers
                                            </Col>
                                            <Col>
                                                <Button
                                                    className="rounded-pill"
                                                    color="primary"
                                                    onClick={() => requestSupportBtnHandler()}
                                                >
                                                    <Icon icon="notifications" /> Request support
                                                </Button>
                                            </Col>
                                        </Row>
                                    </RichPanelHeader>
                                    {supportType !== "Production" && (
                                        <div className="text-center p-4">
                                            <div className="bg-faded-success hstack align-items-center rounded-pill p-1">
                                                <div className="px-4 flex-grow-1 text-center lh-1">
                                                    <strong className="fs-3">Get 2 hour SLA</strong>
                                                    <br />
                                                    <small>We’re here for you 24/7</small>
                                                </div>
                                                <Button color="success" className="px-4 rounded-pill" size="lg">
                                                    <Icon icon="upgrade-arrow" /> <strong>Upgrade to Production</strong>
                                                </Button>
                                            </div>
                                        </div>
                                    )}
                                </>
                            )}
                        </TabPane>
                    </TabContent>
                </Card>
            </div>
            <ReplaceLicenseModal
                showReplaceLicenseModal={showReplaceLicenseModal}
                toggleShowReplaceLicenseModal={toggleShowReplaceLicenseModal}
            />

            <ChangelogModal
                showChangelogModal={showChangelogModal}
                toggleShowChangelogModal={toggleShowChangelogModal}
                changelogDocsUrl={changelogDocsUrl}
                updateInstructionsUrl={updateInstructionsUrl}
            />

            <RequestSupportModal
                showRequestSupportModal={showRequestSupportModal}
                toggleShowRequestSupportModal={toggleShowRequestSupportModal}
                supportId={supportId}
                licenseId={licenseId}
            />
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

interface ReplaceLicenseModalProps {
    showReplaceLicenseModal: boolean;
    toggleShowReplaceLicenseModal: () => void;
}

function ReplaceLicenseModal(props: ReplaceLicenseModalProps) {
    const { showReplaceLicenseModal, toggleShowReplaceLicenseModal } = props;

    return (
        <Modal
            isOpen={showReplaceLicenseModal}
            toggle={toggleShowReplaceLicenseModal}
            wrapClassName="bs5"
            centered
            size="lg"
            contentClassName={`modal-border bulge-primary`}
        >
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon="license" color="warning" className="fs-1" margin="m-0" />
                </div>

                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggleShowReplaceLicenseModal} />
                </div>
                <div data-bind="visible: licenseType() !== 'None'" className="d-flex flex-vertical">
                    <h2>Current license ID</h2>
                    <strong>12345678-1234-1234-1234-123456789101</strong>
                </div>

                <div className="text-center">Can we use old modal or do we need to move it to React?</div>
                <div>
                    <Input type="textarea" name="text" id="messageText" rows={10} />
                </div>
            </ModalBody>
            <ModalFooter>
                <Button color="primary" className="rounded-pill px-3">
                    <Icon icon="check" />
                    Submit
                </Button>
            </ModalFooter>
        </Modal>
    );
}
interface ChangelogModalProps {
    showChangelogModal: boolean;
    toggleShowChangelogModal: () => void;
    changelogDocsUrl: string;
    updateInstructionsUrl: string;
}

function ChangelogModal(props: ChangelogModalProps) {
    const { showChangelogModal, toggleShowChangelogModal, changelogDocsUrl, updateInstructionsUrl } = props;

    return (
        <Modal
            isOpen={showChangelogModal}
            toggle={toggleShowChangelogModal}
            wrapClassName="bs5"
            centered
            size="lg"
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
                <div>
                    <h3>
                        <strong className="text-warning">NEW</strong> - 6.0.0 (60002) - 2023/10/02{" "}
                        <a href={changelogDocsUrl}>
                            <Icon icon="newtab" />
                        </a>
                    </h3>
                    <div className="d-flex gap-3">
                        {/* TODO add messages if no downgrade or needs licesne upgrade*/}
                        <div className="well px-3 py-1 small rounded-pill" id="updateDowngradeInfo">
                            <Icon icon="check" color="success" /> Can downgrade
                        </div>
                        <UncontrolledPopover
                            trigger="hover"
                            className="bs5"
                            placement="top"
                            target="updateDowngradeInfo"
                        >
                            <div className="px-2 py-1">This update is safe to revert to previous version</div>
                        </UncontrolledPopover>
                        <div className="well px-3 py-1 small rounded-pill" id="updateLicenseInfo">
                            <Icon icon="check" color="success" /> License compatible
                        </div>
                        <UncontrolledPopover trigger="hover" className="bs5" placement="top" target="updateLicenseInfo">
                            <div className="px-2 py-1">This update is compatible with your licesne</div>
                        </UncontrolledPopover>
                    </div>
                    <div className="mt-4 vstack gap-2">
                        {/* Parsed HTML */}
                        <h3>Features</h3>
                        <ul>
                            <li>
                                <code>[Corax]</code> new search & indexing engine. More <a href="#">here</a>
                            </li>
                            <li>
                                <code>[Sharding]</code> added &apos;sharding&apos; feature. More <a href="#">here</a>
                            </li>
                            <li>
                                <code>[Queue Sinks]</code> added Kafka and RabbitMQ sink. More <a href="#">here</a>
                            </li>
                            <li>
                                <code>[Data Archival]</code> added &apos;data archival&apos; feature. More{" "}
                                <a href="#">here</a>
                            </li>
                        </ul>
                        <h3>Upgrading from previous versions</h3>
                        <ul>
                            <li>
                                4.x and 5.x licenses will not work with 6.x products and need to be converted via
                                dedicated tool available <a href="#">here</a>. After conversion license will continue
                                working with previous versions of the product, but can be also used with 6.x ones.
                            </li>
                            <li>
                                please refer to our <a href="#">Server migration guide</a> before proceeding with Server
                                update and check our list of Server breaking changes available <a href="#">here</a> and
                                Client API breaking changes available <a href="#">here</a>
                            </li>
                        </ul>
                        <h3>Server</h3>
                        <ul>
                            <li>
                                <code>[Backups]</code> switched FTP backup implementation to use &apos;FluentFTP&apos;
                            </li>
                            <li>
                                <code>[Configuration]</code> changed default
                                &apos;Indexing.OrderByTicksAutomaticallyWhenDatesAreInvolved&apos; value to
                                &apos;true&apos;
                            </li>
                            <li>
                                <code>[ETL]</code> OLAP ETL uses latest Parquet.Net package
                            </li>
                            <li>
                                <code>[ETL]</code> removed load error tolerance
                            </li>
                            <li>
                                <code>[Graph API]</code> removed support
                            </li>
                            <li>
                                <code>[Indexes]</code> new auto indexes will detect DateOnly and TimeOnly automatically
                            </li>
                            <li>
                                <code>[Indexes]</code> added the ability to &apos;test index&apos;. More here
                            </li>
                            <li>
                                <code>[JavaScript]</code> updated Jint to newest version
                            </li>
                            <li>
                                <code>[Monitoring]</code> added OIDs to track certificate expiration and usage
                            </li>
                            <li>
                                <code>[Querying]</code> when two boolean queries are merged in Lucene, boosting should
                                be taken into account properly to avoid merging queries with different boost value
                            </li>
                            <li>
                                <code>[Voron]</code> performance improvements
                            </li>
                        </ul>
                        <h3>Client API</h3>
                        <ul>
                            <li>
                                [Compare Exchange] added support for creating an array as a value in
                                &apos;PutCompareExchangeValueOperation&apos;
                            </li>
                            <li>
                                [Compare Exchange] compare exchange includes should not override already tracked compare
                                exchange values in session to match behavior of regular entities
                            </li>
                            <li>[Conventions] HttpVersion was switched to 2.0</li>
                            <li>
                                [Conventions] removed &apos;UseCompression&apos; and introduced
                                &apos;UseHttpCompression&apos; and &apos;UseHttpDecompression&apos;
                            </li>
                            <li>
                                [Conventions] introduced &apos;DisposeCertificate&apos; with default value set to
                                &apos;true&apos; to help users mitigate the X509Certificate2 leak. More info here
                            </li>
                            <li>
                                [Database] introduced &apos;DatabaseRecordBuilder&apos; for more fluent database record
                                creation
                            </li>
                            <li>[Facets] removed FacetOptions from RangeFacets</li>
                            <li>[Graph API] removed support</li>
                            <li>[Patching] JSON Patch will use conventions when serializing operations</li>
                            <li>[Session] private fields no longer will be picked when projecting from type</li>
                            <li>
                                [Session] taking into account &apos;PropertyNameConverter&apos; when querying and
                                determining field names
                            </li>
                            <li>
                                [Session] when a document has an embedded object with &apos;Id&apos; property we will
                                detect that this is not root object to avoid generating &apos;id(doc)&apos; method there
                                for projection
                            </li>
                            <li>[Session] no tracking session will throw if any includes are used</li>
                            <li>removed obsoletes and marked a lot of types as sealed and internal</li>
                            <li>changed a lot of count and paging related properties and fields from int32 to int64</li>
                        </ul>
                        <h3>Studio</h3>
                        <ul>
                            <li>[Dashboard] removed Server dashboard</li>
                            <li>[Design] refreshed L&F</li>
                        </ul>
                        <h3>Test Driver</h3>
                        <ul>
                            <li>added &apos;PreConfigureDatabase&apos; method</li>
                        </ul>
                        <h3>Other</h3>
                        <ul>
                            <li>[Containers] docker will use non-root user. More info and migration guide here</li>
                        </ul>
                    </div>
                </div>
            </ModalBody>
            <ModalFooter>
                <Button color="secondary" outline onClick={toggleShowChangelogModal} className="rounded-pill px-3">
                    Close
                </Button>
                <FlexGrow />
                <Button color="secondary" outline className="rounded-pill px-3" href={changelogDocsUrl}>
                    Version history <Icon icon="newtab" margin="ms-1" />
                </Button>
                <Button color="primary" className="rounded-pill px-3" href={updateInstructionsUrl}>
                    Update instructions <Icon icon="newtab" margin="ms-1" />
                </Button>
            </ModalFooter>
        </Modal>
    );
}

interface RequestSupportModalProps {
    showRequestSupportModal: boolean;
    toggleShowRequestSupportModal: () => void;
    supportId: string;
    licenseId: string;
}

function RequestSupportModal(props: RequestSupportModalProps) {
    const { showRequestSupportModal, toggleShowRequestSupportModal, supportId, licenseId } = props;

    const { value: includeDebugPackage, toggle: toggleIncludeDebugPackage } = useBoolean(false);
    const { value: includeAllDatabases, toggle: toggleIncludeAllDatabases } = useBoolean(true);

    return (
        <Modal
            isOpen={showRequestSupportModal}
            toggle={toggleShowRequestSupportModal}
            wrapClassName="bs5"
            centered
            size="lg"
            contentClassName={`modal-border bulge-primary`}
        >
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon="support" color="primary" className="fs-1" margin="m-0" />
                </div>

                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggleShowRequestSupportModal} />
                </div>
                <div className="text-center lead">Request support</div>

                <Form className="vstack gap-2">
                    <FormGroup>
                        <Label for="contactEmail">Contact email</Label>
                        <Input
                            type="email"
                            name="contactEmail"
                            value="defaultEmailAssignedToLicense@client.com"
                            placeholder="Email"
                        />
                    </FormGroup>
                    <Row>
                        <Col>
                            <FormGroup>
                                <Label for="supportId">Support ID</Label>
                                <Input type="number" name="supportId" value={supportId} disabled />
                            </FormGroup>
                        </Col>
                        <Col>
                            <FormGroup>
                                <Label for="LicenseId">License ID</Label>
                                <Input type="text" name="supportId" value={licenseId} disabled />
                            </FormGroup>
                        </Col>
                    </Row>
                    <FormGroup>
                        <Label for="messageText">
                            Message <span className="text-muted">(optional)</span>
                        </Label>
                        <Input type="textarea" name="text" id="messageText" rows={10} />
                    </FormGroup>
                    <div className="well p-3 rounded-2">
                        <Checkbox size="lg" selected={includeDebugPackage} toggleSelection={toggleIncludeDebugPackage}>
                            Include debug package
                        </Checkbox>
                        <Collapse isOpen={includeDebugPackage}>
                            <div className="py-2">
                                <Switch selected={includeAllDatabases} toggleSelection={toggleIncludeAllDatabases}>
                                    Include all databases
                                </Switch>
                                <Collapse isOpen={!includeAllDatabases}>
                                    <div className="vstack">
                                        <Checkbox selected={null} toggleSelection={null}>
                                            Database1
                                        </Checkbox>
                                        <Checkbox selected={null} toggleSelection={null}>
                                            Database2
                                        </Checkbox>
                                        <Checkbox selected={null} toggleSelection={null}>
                                            Database3
                                        </Checkbox>
                                    </div>
                                </Collapse>
                                <div className="d-flex gap-4">
                                    <Checkbox selected={null} toggleSelection={null}>
                                        Server
                                    </Checkbox>
                                    <Checkbox selected={null} toggleSelection={null}>
                                        Databases
                                    </Checkbox>
                                    <Checkbox selected={null} toggleSelection={null}>
                                        Logs
                                    </Checkbox>
                                </div>
                            </div>
                        </Collapse>
                    </div>
                </Form>
            </ModalBody>
            <ModalFooter>
                <Button color="secondary" outline onClick={toggleShowRequestSupportModal} className="rounded-pill px-3">
                    Close
                </Button>
                <Button color="primary" className="rounded-pill px-3">
                    <Icon icon="support" />
                    Request support
                </Button>
            </ModalFooter>
        </Modal>
    );
}

export type AvailabilityValue = boolean | number | string;
interface ValueData {
    value: AvailabilityValue;
    overwrittenValue?: AvailabilityValue;
}

export interface FeatureAvailabilityData {
    featureName?: string;

    community: ValueData;
    professional?: ValueData;
    enterprise: ValueData;
}
