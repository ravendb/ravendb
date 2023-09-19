import classNames from "classnames";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { uniqueId } from "lodash";
import React, { ReactNode } from "react";
import { Alert, Button, Table, UncontrolledPopover, UncontrolledTooltip } from "reactstrap";
import IconName from "typings/server/icons";
import { licenseSelectors } from "./shell/licenseSlice";
import { Icon } from "./Icon";
import "./FeatureAvailabilitySummary.scss";
import { AccordionItemWrapper } from "./AboutView";

type AvailabilityValue = boolean | number | string;

interface ValueData {
    value: AvailabilityValue;
    overwrittenValue?: AvailabilityValue;
}

export interface FeatureAvailabilityData {
    featureName?: string;
    featureIcon?: IconName;
    community: ValueData;
    professional?: ValueData;
    enterprise: ValueData;
}

interface FeatureAvailabilitySummaryProps {
    data: FeatureAvailabilityData[];
}

export function FeatureAvailabilitySummary(props: FeatureAvailabilitySummaryProps) {
    const { data } = props;

    const currentLicense = useAppSelector(licenseSelectors.licenseType);
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

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
                <Alert color="danger" className="text-center mb-4">
                    No license detected, you are using RavenDB using <strong>AGPL v3 License</strong>.
                    <br />
                    Community license feature restrictions are applied.
                </Alert>
            )}
            <div className="feature-availability-table">
                <Table>
                    <thead>
                        <tr>
                            <th className="p-0"></th>
                            {licenseTypes.map((licenseType) => {
                                if (currentLicense === "Essential" && licenseType === "Community") {
                                    return (
                                        <th key="Essential" className="community current">
                                            <Icon icon="circle-filled" className="license-dot" /> Essential
                                        </th>
                                    );
                                }
                                return (
                                    <th
                                        key={licenseType}
                                        className={classNames("position-relative", licenseType.toLowerCase(), {
                                            current:
                                                currentLicense === licenseType ||
                                                (currentLicense === "None" && licenseType === "Community") ||
                                                (currentLicense === "Community" && licenseType === "Free") ||
                                                (currentLicense === "Enterprise" && licenseType === "Production"),
                                        })}
                                    >
                                        <Icon icon="circle-filled" className="license-dot" />
                                        {licenseType}
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
                                                            <strong>Enterprise License features</strong>
                                                            <br /> but is{" "}
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
                            <tr key={idx}>
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
                                        current:
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
                                            current: currentLicense === "Professional",
                                        })}
                                    >
                                        {formatAvailabilityValue(data.professional)}
                                    </td>
                                )}
                                <td className={classNames("enterprise", { current: currentLicense === "Enterprise" })}>
                                    {formatAvailabilityValue(data.enterprise)}
                                </td>
                                {currentLicense === "Developer" && (
                                    <td
                                        className={classNames("developer", { current: currentLicense === "Developer" })}
                                    >
                                        {formatAvailabilityValue(data.enterprise)}
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
                                            current:
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
                <div className="hstack gap-4 justify-content-center mt-4">
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
            {currentLicense !== "Enterprise" && currentLicense !== "None" && (
                <div className="hstack gap-4 justify-content-center mt-4">
                    Upgrade License
                    <a
                        href={buyLink}
                        target="_blank"
                        color="primary"
                        className="btn btn-primary btn-lg rounded-pill px-4"
                    >
                        <Icon icon="license" margin="me-3" />
                        Pricing plans
                    </a>
                </div>
            )}
        </>
    );
}

function formatAvailabilityValue(data: ValueData): ReactNode {
    const value = data.overwrittenValue ?? data.value;

    let formattedValue: ReactNode = value;

    if (value === true) {
        formattedValue = <Icon icon="check" margin="m-0" color="success" />;
    }
    if (value === false) {
        formattedValue = <Icon icon="cancel" margin="m-0" color="danger" />;
    }
    if (value === Infinity) {
        formattedValue = <Icon icon="infinity" margin="m-0" />;
    }

    if (data.overwrittenValue == null) {
        return formattedValue;
    }

    const id = "overwritten-Availability-value-" + uniqueId();

    return (
        <>
            {formattedValue}
            <Icon id={id} icon="info" color="info" />
            <UncontrolledTooltip target={id}>
                Default value for your license is {data.value.toString()}.
            </UncontrolledTooltip>
        </>
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
