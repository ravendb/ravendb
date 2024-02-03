import { Button, Card, CardBody, Col, Row } from "reactstrap";
import { aboutPageUrls, ConnectivityStatus, OverallInfoItem } from "components/pages/resources/about/partials/common";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import React from "react";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import licenseModel from "models/auth/licenseModel";
import { AsyncState } from "react-async-hook";

interface SupportSummaryProps {
    asyncCheckLicenseServerConnectivity: AsyncState<ConnectivityStatus>;
}

export function SupportSummary(props: SupportSummaryProps) {
    const license = useAppSelector(licenseSelectors.status);
    const support = useAppSelector(licenseSelectors.support);
    const supportType = licenseModel.supportLabelProvider(license, support);

    const { asyncCheckLicenseServerConnectivity } = props;

    const hideSupportStatus =
        asyncCheckLicenseServerConnectivity.status === "error" ||
        (asyncCheckLicenseServerConnectivity.status === "success" &&
            !asyncCheckLicenseServerConnectivity.result.connected);

    return (
        <Card>
            <CardBody>
                <h4>Support</h4>
                <Row>
                    <OverallInfoItem icon="support" label="Support type">
                        {hideSupportStatus ? (
                            <span className="text-warning" id="connectivityException">
                                <Icon icon="warning" />
                                <small>
                                    Unable to reach the RavenDB License Server at <code>api.ravendb.net</code>
                                </small>
                            </span>
                        ) : (
                            <span
                                className={classNames(
                                    { "text-professional": supportType === "Professional" },
                                    { "text-enterprise": supportType === "Production" }
                                )}
                            >
                                {supportType}
                            </span>
                        )}
                    </OverallInfoItem>

                    <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                        {/* TODO we hide this for now 
                        <Button
                            className="rounded-pill"
                            color={paidSupportAvailable ? "primary" : "secondary"}
                            onClick={() => requestSupportBtnHandler()}
                        >
                            <Icon icon="notifications" /> Request support
                        </Button>*/}
                        <Button outline className="rounded-pill" href={aboutPageUrls.gitHubDiscussions} target="_blank">
                            <Icon icon="group" /> Ask community <Icon icon="newtab" margin="ms-1" />
                        </Button>
                    </Col>
                </Row>
            </CardBody>
        </Card>
    );
}
