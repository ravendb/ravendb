import React from "react";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import { AsyncState } from "react-async-hook";
import { aboutPageUrls, ConnectivityStatus } from "components/pages/resources/about/partials/common";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import licenseModel from "models/auth/licenseModel";
import classNames from "classnames";
import { useAboutPage } from "components/pages/resources/about/useAboutPage";
import { useRavenLink } from "hooks/useRavenLink";
import { linkTo } from "@storybook/addon-links";

interface ContactSupportContentProps {
    asyncCheckLicenseServerConnectivity: AsyncState<ConnectivityStatus>;
}

export function ContactSupportContent(props: ContactSupportContentProps) {
    const license = useAppSelector(licenseSelectors.status);
    const licenseId = useAppSelector(licenseSelectors.statusValue("Id"));
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const support = useAppSelector(licenseSelectors.support);
    const supportType = licenseModel.supportLabelProvider(license, support);
    const isPaidSupport = ["Professional", "Production", "Partial"].includes(supportType);

    const { asyncCheckLicenseServerConnectivity } = useAboutPage();

    const hideSupportStatus =
        asyncCheckLicenseServerConnectivity.status === "error" ||
        (asyncCheckLicenseServerConnectivity.status === "success" &&
            !asyncCheckLicenseServerConnectivity.result.connected);

    const supportPlansUrl = useRavenLink({ hash: "2DW5F4" });
    const cloudTermsUrl = useRavenLink({ hash: "75DJRY" });
    const onPremiseTermsUrl = useRavenLink({ hash: "R1OFBF" });
    const gitHubCommunityUrl = useRavenLink({ hash: "ITXUEA" });
    const cloudRequestSupport = useRavenLink({ hash: "2YGOL1" });
    const onPremiseRequestSupport = "https://ravendb.net/support/supportrequest?licenseId=" + licenseId;

    const requestSupportUrl = isCloud ? cloudRequestSupport : onPremiseRequestSupport;
    const termsUrl = isCloud ? cloudTermsUrl : onPremiseTermsUrl;

    return (
        <>
            <ul className="action-menu__list">
                <p className="m-0">
                    {hideSupportStatus ? (
                        <span className="text-warning">
                            <Icon icon="warning" />
                            <small>
                                Unable to reach the RavenDB License Server at <code>api.ravendb.net</code>
                            </small>
                        </span>
                    ) : (
                        <span>
                            You are using{" "}
                            <strong
                                className={classNames(
                                    { "text-professional": supportType === "Professional" },
                                    { "text-enterprise": supportType === "Production" },
                                    { "text-community": supportType === "Community" }
                                )}
                            >
                                {isCloud && supportType === "Production" ? "Cloud Support" : supportType}
                            </strong>{" "}
                            plan
                        </span>
                    )}
                </p>
                {isPaidSupport && !hideSupportStatus ? (
                    <>
                        {(supportType === "Professional" || supportType === "Production") && (
                            <div className="d-flex align-items-center gap-1">
                                <Icon icon="clock" />
                                <div className="d-flex flex-column">
                                    <span className="fw-bold lh-base">
                                        {supportType === "Professional" ? "Next day SLA" : "2 hour SLA"}
                                    </span>
                                    <small className="text-muted lh-1">
                                        {supportType === "Professional"
                                            ? "Sun-Fri, Business Hours"
                                            : "24/7 availability"}
                                    </small>
                                </div>
                            </div>
                        )}
                        <a href={termsUrl} target="_blank" className="d-flex align-items-center gap-1 w-fit-content">
                            <Icon icon="terms" />
                            Terms and conditions
                        </a>
                        <li
                            className={classNames(
                                "mt-1 action-menu__list-item",
                                isCloud ? "action-menu__list-item--cloud" : "action-menu__list-item--primary"
                            )}
                            role="button"
                            title="Request support"
                            onClick={() => window.open(requestSupportUrl, "_blank")}
                        >
                            <Icon icon="notifications" margin="m-0" />
                            Request support
                            <FlexGrow />
                            <Icon icon="newtab" margin="m-0" />
                        </li>
                    </>
                ) : (
                    <>
                        <p className="m-0">
                            Get fast and comprehensive help from fellow RavenDB users and developers in our community
                            forum.
                        </p>
                        <li
                            className="action-menu__list-item action-menu__list-item--primary mt-1"
                            role="button"
                            title="Go to GitHub discussions"
                        >
                            <Icon icon="github" margin="m-0" />
                            GitHub Discussions
                            <FlexGrow />
                            <Icon icon="newtab" margin="m-0" />
                        </li>
                    </>
                )}
            </ul>
            <div className="action-menu__footer">
                <small className="text-muted lh-1">
                    <Icon icon="support" />
                    Get more information about our{" "}
                    <a href={supportPlansUrl} target="_blank">
                        support plans
                    </a>
                </small>
                {isPaidSupport && !hideSupportStatus && (
                    <small className="text-muted lh-1 mt-1">
                        <Icon icon="github" />
                        Join our{" "}
                        <a href={gitHubCommunityUrl} target="_blank">
                            GitHub community
                        </a>
                    </small>
                )}
            </div>
        </>
    );
}
