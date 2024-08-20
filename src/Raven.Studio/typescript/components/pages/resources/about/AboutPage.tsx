import React, { useState } from "react";
import { Badge, Card, Col, Row, TabContent, TabPane } from "reactstrap";
import { Icon } from "components/common/Icon";
import "./AboutPage.scss";

import classNames from "classnames";
import { LicenseSummary } from "components/pages/resources/about/partials/LicenseSummary";
import { VersionsSummary } from "components/pages/resources/about/partials/VersionsSummary";
import { SupportSummary } from "components/pages/resources/about/partials/SupportSummary";
import { AboutFooter } from "components/pages/resources/about/partials/AboutFooter";
import { PassiveState } from "components/pages/resources/about/partials/PassiveState";
import { LicenseDetails } from "components/pages/resources/about/partials/LicenseDetails";
import { ChangeLogModal } from "components/pages/resources/about/partials/ChangeLogModal";
import { SupportDetails } from "components/pages/resources/about/partials/SupportDetails";
import { useAboutPage } from "components/pages/resources/about/useAboutPage";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import licenseModel from "models/auth/licenseModel";

interface AboutPageProps {
    initialChangeLogMode?: "whatsNew" | "changeLog" | "hidden";
}

export function AboutPage({ initialChangeLogMode = "hidden" }: AboutPageProps) {
    const licenseRegistered = useAppSelector(licenseSelectors.licenseRegistered);
    const license = useAppSelector(licenseSelectors.status);
    const support = useAppSelector(licenseSelectors.support);
    const supportType = licenseModel.supportLabelProvider(license, support);
    const paidSupportAvailable = supportType === "Community";

    const { asyncFetchLatestVersion, asyncCheckLicenseServerConnectivity, asyncGetConfigurationSettings } =
        useAboutPage();

    const [activeTab, setActiveTab] = useState<"license" | "support">("license");
    const [changeLogMode, setChangeLogMode] = useState<"whatsNew" | "changeLog" | "hidden">(initialChangeLogMode);

    const refreshLatestVersion = async () => {
        await asyncFetchLatestVersion.execute(true);
    };

    const refreshLicenseServerConnectivity = async () => {
        await asyncCheckLicenseServerConnectivity.execute();
    };

    return (
        <div className="d-flex flex-column">
            <div className="m-4 vstack flex-wrap gap-4 flex-grow-1 align-items-stretch justify-content-evenly about-page">
                <div className="hstack flex-wrap gap-4 flex-grow-1 align-items-stretch">
                    <div className="d-flex" style={{ maxWidth: "600px", minWidth: "540px" }}>
                        <div className="vstack gap-4 flex-grow-0">
                            <PassiveState />
                            <LicenseSummary
                                recheckConnectivity={refreshLicenseServerConnectivity}
                                asyncCheckLicenseServerConnectivity={asyncCheckLicenseServerConnectivity}
                                asyncGetConfigurationSettings={asyncGetConfigurationSettings}
                            />
                            <VersionsSummary
                                refreshLatestVersion={refreshLatestVersion}
                                asyncLatestVersion={asyncFetchLatestVersion}
                                showWhatsNewModal={() => setChangeLogMode("whatsNew")}
                                showChangeLogModal={() => setChangeLogMode("changeLog")}
                            />
                            <SupportSummary asyncCheckLicenseServerConnectivity={asyncCheckLicenseServerConnectivity} />
                        </div>
                    </div>
                    <Card className="flex-grow">
                        <Row className="about-page-tabs">
                            <Col>
                                <a
                                    className={classNames("p-3 no-decor", {
                                        "active bg-faded-primary": activeTab === "license",
                                    })}
                                    onClick={() => setActiveTab("license")}
                                >
                                    <Icon
                                        icon="license"
                                        className="circle-border fs-2"
                                        color={licenseRegistered ? "success" : "warning"}
                                        margin="me-2"
                                    />
                                    <span className="fs-3">License details</span>
                                    {/* TODO show if you some extra options to enable in cloud
                                <Badge className="rounded-pill py-1 ms-1 align-self-start" color="primary">
                                    1
                                </Badge> */}
                                </a>
                            </Col>
                            <Col>
                                <a
                                    className={classNames("p-3 no-decor", {
                                        "active bg-faded-info": activeTab === "support",
                                    })}
                                    onClick={() => setActiveTab("support")}
                                >
                                    <Icon icon="support" className="circle-border" margin="fs-2 me-2" />
                                    <span className="fs-3">Support plan</span>
                                    {paidSupportAvailable && (
                                        <Badge className="rounded-pill py-1 ms-1 align-self-start" color="primary">
                                            1
                                        </Badge>
                                    )}
                                </a>
                            </Col>
                        </Row>

                        <TabContent activeTab={activeTab} className="flex-grow-1">
                            <TabPane tabId="license">
                                <LicenseDetails />
                            </TabPane>
                            <TabPane tabId="support">
                                <SupportDetails
                                    asyncCheckLicenseServerConnectivity={asyncCheckLicenseServerConnectivity}
                                />
                            </TabPane>
                        </TabContent>
                    </Card>
                </div>
            </div>
            <AboutFooter />
            {changeLogMode !== "hidden" && (
                <ChangeLogModal mode={changeLogMode} onClose={() => setChangeLogMode("hidden")} />
            )}

            {/* TODO we hide this for now
            <RequestSupportModal
                visible={showRequestSupportModal}
                toggle={toggleShowRequestSupportModal}
                supportId={supportId}
                licenseId={licenseId}
            />*/}
        </div>
    );
}
