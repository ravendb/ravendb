import classNames from "classnames";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { uniqueId } from "lodash";
import React, { ReactNode } from "react";
import { Alert, Button, Table, UncontrolledTooltip } from "reactstrap";
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
                                        className={classNames(licenseType.toLowerCase(), {
                                            current:
                                                currentLicense === licenseType ||
                                                (currentLicense === "None" && licenseType === "Community"),
                                        })}
                                    >
                                        <Icon icon="circle-filled" className="license-dot" />
                                        {licenseType}
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
                                            current: currentLicense === licenseType,
                                        })}
                                    >
                                        {currentLicense === licenseType && "current"}
                                    </td>
                                );
                            })}
                        </tr>
                    </tbody>
                </Table>
            </div>
            {currentLicense === "None" && (
                <div className="hstack gap-4 justify-content-center mt-4">
                    <Button href={buyLink} target="_blank" color="primary" size="lg" className="rounded-pill px-4">
                        <Icon icon="license" margin="me-3" />
                        Get License
                    </Button>
                </div>
            )}
            {currentLicense !== "Enterprise" && currentLicense !== "None" && (
                <div className="hstack gap-4 justify-content-center mt-4">
                    Upgrade License
                    <Button href={buyLink} target="_blank" color="primary" size="lg" className="rounded-pill px-4">
                        <Icon icon="license" margin="me-3" />
                        Pricing plans
                    </Button>
                </div>
            )}
        </>
    );
}

function formatAvailabilityValue(data: ValueData): ReactNode {
    const value = data.overwrittenValue ?? data.value;

    let formattedValue: ReactNode = value;

    if (value === true) {
        formattedValue = <Icon icon="check" color="success" />;
    }
    if (value === false) {
        formattedValue = <Icon icon="cancel" color="danger" />;
    }
    if (value === Infinity) {
        formattedValue = <Icon icon="infinity" />;
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
        >
            <FeatureAvailabilitySummary data={data} />
        </AccordionItemWrapper>
    );
}
