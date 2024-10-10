import React, { ReactNode } from "react";
import { RichPanelHeader } from "components/common/RichPanel";
import { Button, Col, Row } from "reactstrap";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import IconName from "../../../../../../typings/server/icons";
import { aboutPageUrls, ConnectivityStatus } from "components/pages/resources/about/partials/common";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import licenseModel from "models/auth/licenseModel";
import { AsyncState } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";

const supportImg = require("Content/img/pages/about/support.svg");
const supportCharacterImg = require("Content/img/pages/about/supportCharacter.svg");

interface SupportDetailsProps {
    asyncCheckLicenseServerConnectivity: AsyncState<ConnectivityStatus>;
}

export function SupportDetails(props: SupportDetailsProps) {
    const { asyncCheckLicenseServerConnectivity } = props;

    const license = useAppSelector(licenseSelectors.status);
    const licenseId = useAppSelector(licenseSelectors.statusValue("Id"));
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const support = useAppSelector(licenseSelectors.support);
    const supportType = licenseModel.supportLabelProvider(license, support);
    const isPaidSupport = ["Professional", "Production", "Partial"].includes(supportType);

    const upgradeLink = isCloud ? aboutPageUrls.upgradeSupport.cloud : aboutPageUrls.upgradeSupport.onPremise;

    if (asyncCheckLicenseServerConnectivity.status === "loading") {
        return (
            <LazyLoad active>
                <div>Loading placeholder</div>
            </LazyLoad>
        );
    }

    if (asyncCheckLicenseServerConnectivity.status === "error") {
        return (
            <div className="m-3">
                <LoadError />
            </div>
        );
    }

    if (!asyncCheckLicenseServerConnectivity.result.connected) {
        return (
            <div className="m-3">
                <LoadError error="Please check connection to license server." />
            </div>
        );
    }

    const getTypeHeader = () => {
        if (supportType === "Community") {
            return "Free Community Support";
        }
        if (isCloud && supportType === "Production") {
            return "Cloud Support";
        }
        if (support.SupportType === "Extended") {
            return "Extended Support";
        }

        return `${supportType} Support`;
    };

    return (
        <React.Fragment key="support-details">
            <div className="bg-faded-info hstack justify-content-center">
                <img alt="Support Character" src={supportCharacterImg} className="support-character-img mt-4" />
            </div>
            {supportType === "Community" && (
                <>
                    <RichPanelHeader className="text-center p-4">
                        You are using
                        <h2 className="text-info">{getTypeHeader()}</h2>
                        <p>
                            Get help and connect with fellow users and RavenDB developers through our community forum.
                        </p>
                        <Button
                            outline
                            href={aboutPageUrls.askCommunity}
                            className="rounded-pill align-self-center px-3"
                            target="_blank"
                        >
                            <Icon icon="group" /> Ask community <Icon icon="newtab" margin="ms-2" />
                        </Button>
                    </RichPanelHeader>
                    <div className="text-center p-4 vstack align-items-center">
                        <h2 className="hstack justify-content-center">
                            <img alt="Support" src={supportImg} width={70} className="me-4" />
                            Elevate your experience
                        </h2>
                        <div>
                            <p className="max-paragraph-width">
                                RavenDB Support has you covered. Whether it’s a simple question or a mission-critical
                                emergency, our core developers will be on hand to provide expert assistance and advice
                                around the clock.
                            </p>
                            <p>Here’s what you’ll get:</p>
                        </div>
                        <Row className="support-advantages">
                            <SupportAdvantage icon="phone">Phone & Email support</SupportAdvantage>
                            {/* <SupportAdvantage icon="notifications"> TODO we hide this for now*/}
                            {/*    Request support directly from RavenDB Studio*/}
                            {/*</SupportAdvantage>*/}
                            <SupportAdvantage icon="user">Access to RavenDB core developers</SupportAdvantage>
                            <SupportAdvantage icon="clock">
                                Up to 2 hour SLA
                                <br />
                                24/7 availability
                            </SupportAdvantage>
                        </Row>
                        <Button
                            color="success"
                            className="px-4 rounded-pill mt-4"
                            size="lg"
                            href={upgradeLink}
                            target="_blank"
                        >
                            <Icon icon="upgrade-arrow" /> <strong>Upgrade Your Support</strong>
                        </Button>
                    </div>
                </>
            )}
            {supportType !== "Community" && (
                <>
                    <RichPanelHeader className="px-4 py-4 vstack gap-4 justify-content-center">
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
                                            {getTypeHeader()}
                                        </h2>
                                    </div>
                                    <img alt="Support" src={supportImg} width={70} className="ms-4" />
                                </div>
                            </Col>
                            <Col xs={12} sm={6}>
                                <div className="vstack gap-4">
                                    <div className="hstack">
                                        {(supportType === "Professional" || supportType === "Production") && (
                                            <>
                                                <Icon icon="clock" className="fs-2" />

                                                <div className="small me-3">
                                                    {supportType === "Professional" && (
                                                        <>
                                                            <strong>Next business day SLA</strong>
                                                            <div>24/7 availability</div>
                                                        </>
                                                    )}
                                                    {supportType === "Production" && (
                                                        <>
                                                            <strong>2 hour SLA</strong>
                                                            <div>24/7 availability</div>
                                                        </>
                                                    )}
                                                </div>
                                            </>
                                        )}
                                        {isPaidSupport && (
                                            <Button
                                                className="rounded-pill"
                                                color={isCloud ? "cloud" : "primary"}
                                                href={
                                                    isCloud
                                                        ? aboutPageUrls.cloudPortal
                                                        : aboutPageUrls.supportRequest(licenseId)
                                                }
                                                target="_blank"
                                            >
                                                <Icon icon="notifications" /> Request support
                                            </Button>
                                        )}
                                    </div>
                                    <div>
                                        <a
                                            href={
                                                isCloud ? aboutPageUrls.cloudSupportTerms : aboutPageUrls.supportTerms
                                            }
                                            className="d-inline-flex no-decor"
                                        >
                                            <Icon icon="terms" className="fs-2" />
                                            <div className="small">Terms and conditions</div>
                                        </a>
                                    </div>
                                </div>
                            </Col>
                        </Row>
                        <Row className="g-md">
                            <Col className="text-right">
                                Get help and connect with fellow users and RavenDB developers through our community
                                forum.
                            </Col>
                            <Col>
                                <Button
                                    outline
                                    href={aboutPageUrls.askCommunity}
                                    className="rounded-pill align-self-center px-3"
                                    target="_blank"
                                >
                                    <Icon icon="group" /> Ask community <Icon icon="newtab" margin="ms-2" />
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
                                <Button href={upgradeLink} color="success" className="px-4 rounded-pill" size="lg">
                                    <Icon icon="upgrade-arrow" /> <strong>Upgrade to Production</strong>
                                </Button>
                            </div>
                        </div>
                    )}
                </>
            )}
        </React.Fragment>
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
