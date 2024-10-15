import classNames from "classnames";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { uniqueId } from "lodash";
import { ReactNode } from "react";
import { Button, Table, UncontrolledPopover, UncontrolledTooltip } from "reactstrap";
import IconName from "typings/server/icons";
import { licenseSelectors } from "./shell/licenseSlice";
import { Icon } from "./Icon";
import "./FeatureAvailabilitySummary.scss";
import { AccordionItemWrapper } from "./AboutView";
import RichAlert from "components/common/RichAlert";

export type AvailabilityValue = boolean | number | string;

export interface FeatureAvailabilityValueData {
    value: AvailabilityValue;
    overwrittenValue?: AvailabilityValue;
}

export interface FeatureAvailabilityData {
    featureName?: string;
    featureIcon?: IconName;
    community: FeatureAvailabilityValueData;
    professional?: FeatureAvailabilityValueData;
    enterprise: FeatureAvailabilityValueData;
}

interface FeatureAvailabilitySummaryProps {
    data: FeatureAvailabilityData[];
}

export function FeatureAvailabilitySummary(props: FeatureAvailabilitySummaryProps) {
    const { data } = props;

    const currentLicense = useAppSelector(licenseSelectors.licenseType);
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const isIsv = useAppSelector(licenseSelectors.statusValue("IsIsv"));

    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    if (!currentLicense) {
        return null;
    }

    const licenseTypes = isCloud ? ["Free", "Production"] : ["Community", "Professional", "Enterprise"];

    if (currentLicense === "Developer") {
        licenseTypes.push("Developer");
    }

    return (
        <>
            {currentLicense === "None" && (
                <RichAlert variant="danger" className="mb-4">
                    No license detected.
                    <br />
                    You are using RavenDB using <strong>AGPL v3 License</strong>.
                    <br />
                    Community license feature restrictions are applied.
                </RichAlert>
            )}
            <div className="feature-availability-table">
                <Table>
                    <thead>
                        <tr>
                            <th className="p-0"></th>
                            {licenseTypes.map((licenseType) => {
                                if (isIsv && licenseType === "Community") {
                                    return (
                                        <th
                                            key="Essential"
                                            className={classNames("community", {
                                                "current bg-faded-primary": currentLicense === "Essential",
                                            })}
                                        >
                                            <Icon icon="circle-filled" className="license-dot" /> Essential
                                        </th>
                                    );
                                }
                                return (
                                    <th
                                        key={licenseType}
                                        className={classNames("position-relative", licenseType.toLowerCase(), {
                                            "current bg-faded-primary":
                                                currentLicense === licenseType ||
                                                (currentLicense === "None" && licenseType === "Community") ||
                                                (currentLicense === "Community" && licenseType === "Free") ||
                                                (currentLicense === "Enterprise" && licenseType === "Production"),
                                        })}
                                    >
                                        <Icon icon="circle-filled" className="license-dot" />
                                        {licenseType === "Developer" ? <span>Dev</span> : licenseType}
                                        {licenseType === "Developer" && (
                                            <>
                                                <div className="corner-info" id="DevTooltip">
                                                    <Icon icon="info" margin="m-0" />
                                                </div>
                                                <UncontrolledPopover
                                                    placement="top"
                                                    target="DevTooltip"
                                                    trigger="hover"
                                                    className="bs5"
                                                >
                                                    <div className="p-2 text-center">
                                                        <div>
                                                            Developer license enables{" "}
                                                            <strong>Enterprise License features</strong> but is{" "}
                                                            <strong>not applicable for commercial use</strong>.
                                                        </div>

                                                        <Button
                                                            color="link"
                                                            size="sm"
                                                            href="https://ravendb.net/l/FLDLO4#developer"
                                                            target="_blank"
                                                        >
                                                            See details <Icon icon="newtab" margin="ms-1" />
                                                        </Button>
                                                    </div>
                                                </UncontrolledPopover>
                                            </>
                                        )}
                                    </th>
                                );
                            })}
                        </tr>
                    </thead>
                    <tbody>
                        {data.map((data, idx) => (
                            <tr key={idx} className="feature-row">
                                <th className="p-0">
                                    {data.featureName && (
                                        <div className="p-2">
                                            {data.featureIcon && <Icon icon={data.featureIcon} />}
                                            {data.featureName}
                                        </div>
                                    )}
                                </th>
                                <td
                                    className={classNames("community", {
                                        "current bg-faded-primary":
                                            currentLicense === "Community" ||
                                            currentLicense === "Essential" ||
                                            currentLicense === "None",
                                    })}
                                >
                                    {formatAvailabilityValue(data.community)}
                                </td>
                                {!isCloud && (
                                    <td
                                        className={classNames("professional", {
                                            "current bg-faded-primary": currentLicense === "Professional",
                                        })}
                                    >
                                        {formatAvailabilityValue(data.professional)}
                                    </td>
                                )}
                                <td
                                    className={classNames("enterprise", {
                                        "current bg-faded-primary": currentLicense === "Enterprise",
                                    })}
                                >
                                    {formatAvailabilityValue(data.enterprise, isCloud)}
                                </td>
                                {currentLicense === "Developer" && (
                                    <td
                                        className={classNames("developer", {
                                            "current bg-faded-primary": currentLicense === "Developer",
                                        })}
                                    >
                                        {formatAvailabilityValue(data.enterprise, isCloud)}
                                    </td>
                                )}
                            </tr>
                        ))}
                        <tr className="current-indicator-row">
                            <th className="p-0"></th>
                            {licenseTypes.map((licenseType) => {
                                if (
                                    (currentLicense === "Essential" || currentLicense === "None") &&
                                    licenseType === "Community"
                                ) {
                                    return (
                                        <td key="Essential" className="community current">
                                            current
                                        </td>
                                    );
                                }
                                return (
                                    <td
                                        key={licenseType}
                                        className={classNames(licenseType.toLowerCase(), {
                                            "current bg-faded-primary":
                                                currentLicense === licenseType ||
                                                (currentLicense === "Community" && licenseType === "Free") ||
                                                (currentLicense === "Enterprise" && licenseType === "Production"),
                                        })}
                                    >
                                        {(currentLicense === licenseType ||
                                            (currentLicense === "Community" && licenseType === "Free") ||
                                            (currentLicense === "Enterprise" && licenseType === "Production")) &&
                                            "current"}
                                    </td>
                                );
                            })}
                        </tr>
                    </tbody>
                </Table>
            </div>
            {currentLicense === "None" && (
                <div className="hstack gap-4 justify-content-center mt-4 flex-wrap">
                    <a
                        href={buyLink}
                        target="_blank"
                        color="primary"
                        className="btn btn-primary btn-lg rounded-pill px-4"
                    >
                        <Icon icon="license" margin="me-3" />
                        Get License
                    </a>
                </div>
            )}
            {currentLicense !== "Enterprise" && currentLicense !== "None" && <UpgradeLinkSection />}
        </>
    );
}

function formatAvailabilityValue(data: FeatureAvailabilityValueData, canBeEnabledInCloud?: boolean): ReactNode {
    const value = data.overwrittenValue ?? data.value;

    let formattedValue: ReactNode = value;

    if (value === true) {
        formattedValue = <Icon icon="check" margin="m-0" color="success" />;
    }
    if (value === false) {
        if (canBeEnabledInCloud) {
            const cloudOnDemandId = "cloud-on-demand-" + uniqueId();
            return (
                <>
                    <Icon id={cloudOnDemandId} icon="upgrade-arrow" margin="m-0" color="success" />
                    <UncontrolledTooltip target={cloudOnDemandId}>
                        You can enable this feature in RavenDB Cloud Portal or by contacting support.
                    </UncontrolledTooltip>
                </>
            );
        } else {
            formattedValue = <Icon icon="cancel" margin="m-0" color="danger" />;
        }
    }
    if (value === Infinity) {
        formattedValue = <Icon icon="infinity" margin="m-0" />;
    }

    if (data.overwrittenValue == null) {
        return formattedValue;
    }

    const id = "overwritten-availability-value-" + uniqueId();

    return (
        <>
            <div className="overwritten-value">
                {formattedValue}
                <Icon id={id} icon="info" color="info" margin="m-0" />
                <UncontrolledTooltip target={id}>
                    Default value for your license is {data.value.toString()}.
                </UncontrolledTooltip>
            </div>
        </>
    );
}

function UpgradeLinkSection() {
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const ravenBuyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });
    const cloudPricingLink = "https://cloud.ravendb.net/pricing";

    return (
        <div className="hstack gap-4 justify-content-center mt-4 flex-wrap">
            {isCloud ? (
                <>
                    Upgrade Instance
                    <a
                        href={cloudPricingLink}
                        target="_blank"
                        color="primary"
                        className="btn btn-primary btn-lg rounded-pill px-4"
                    >
                        <Icon icon="license" margin="me-3" />
                        Cloud pricing
                    </a>
                </>
            ) : (
                <>
                    Upgrade License
                    <a
                        href={ravenBuyLink}
                        target="_blank"
                        color="primary"
                        className="btn btn-primary btn-lg rounded-pill px-4"
                    >
                        <Icon icon="license" margin="me-3" />
                        Pricing plans
                    </a>
                </>
            )}
        </div>
    );
}

export default function FeatureAvailabilitySummaryWrapper({
    isUnlimited,
    data,
}: FeatureAvailabilitySummaryProps & { isUnlimited: boolean }) {
    return (
        <AccordionItemWrapper
            icon="license"
            color={isUnlimited ? "success" : "warning"}
            heading="Licensing"
            description="See which plans offer this and more exciting features"
            targetId="licensing"
            className={isUnlimited ? null : "license-limited"}
        >
            <FeatureAvailabilitySummary data={data} />
        </AccordionItemWrapper>
    );
}
