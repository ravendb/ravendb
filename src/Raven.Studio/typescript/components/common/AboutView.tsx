import React, { ReactNode, useEffect, useState } from "react";
import "./AboutView.scss";
import {
    AccordionBody,
    AccordionHeader,
    AccordionItem,
    Badge,
    Button,
    PopoverBody,
    Table,
    UncontrolledAccordion,
    UncontrolledPopover,
} from "reactstrap";
import classNames from "classnames";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";
import { TextColor } from "components/models/common";
import { todo } from "common/developmentHelper";
import { uniqueId } from "lodash";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "./shell/licenseSlice";
import "./FeatureAvailabilityTable.scss";

interface AboutViewProps {
    children?: ReactNode | ReactNode[];
    className?: string;
    defaultOpen?: boolean;
}

interface AboutViewHeadingProps {
    title: string;
    icon: IconName;
    badgeText?: string;
    marginBottom?: number;
}

export interface FeatureAvailabilityData {
    featureName?: ReactNode;
    community: boolean | number | string;
    professional: boolean | number | string;
    enterprise: boolean | number | string;
}

interface FeatureAvailabilityTableProps {
    availabilityData: FeatureAvailabilityData[];
}

const FeatureAvailabilityTable = (props: FeatureAvailabilityTableProps) => {
    const { availabilityData } = props;

    const currentLicense = useAppSelector(licenseSelectors.licenseType);

    const licenseTypes: Raven.Server.Commercial.LicenseType[] = ["Community", "Professional", "Enterprise"];

    if (currentLicense === "Developer") {
        licenseTypes.push("Developer");
    }

    return (
        <Table className="feature-availability-table">
            <thead>
                <tr>
                    <th></th>
                    {licenseTypes.map((licenseType) => {
                        if (currentLicense === "Essential" && licenseType === "Community") {
                            return (
                                <th key="Essential" className="current">
                                    Essential
                                </th>
                            );
                        }
                        return (
                            <th key={licenseType} className={classNames({ current: currentLicense === licenseType })}>
                                {licenseType}
                            </th>
                        );
                    })}
                </tr>
            </thead>
            <tbody>
                {availabilityData.map((data, idx) => (
                    <tr key={idx}>
                        <td>{data.featureName}</td>
                        <td
                            className={classNames({
                                current: currentLicense === "Community" || currentLicense === "Essential",
                            })}
                        >
                            {formatAvailabilityValue(data.community)}
                        </td>
                        <td className={classNames({ current: currentLicense === "Professional" })}>
                            {formatAvailabilityValue(data.professional)}
                        </td>
                        <td className={classNames({ current: currentLicense === "Enterprise" })}>
                            {formatAvailabilityValue(data.enterprise)}
                        </td>
                        {currentLicense === "Developer" && (
                            <td className={classNames({ current: currentLicense === "Developer" })}>
                                {formatAvailabilityValue(data.enterprise)}
                            </td>
                        )}
                    </tr>
                ))}
            </tbody>
        </Table>
    );
};

function formatAvailabilityValue(value: boolean | number | string): ReactNode {
    if (value === true) {
        return <Icon icon="check" color="success" />;
    }
    if (value === false) {
        return <Icon icon="cancel" color="danger" />;
    }
    if (value === Infinity) {
        return <Icon icon="infinity" />;
    }

    return value;
}

const AboutViewHeading = (props: AboutViewHeadingProps) => {
    const { title, icon, badgeText, marginBottom } = props;
    return (
        <h2 className={classNames("d-flex align-items-center gap-1 flex-wrap", `mb-${marginBottom ?? 5}`)}>
            <Icon icon={icon} /> {title}{" "}
            {badgeText != null && (
                <Badge color="faded-primary" className="about-view-title-badge">
                    {badgeText}
                </Badge>
            )}
        </h2>
    );
};

const aboutViewId = "about-view";

const AboutViewFloating = (props: AboutViewProps) => {
    const { children, className, defaultOpen } = props;
    const [target, setTarget] = useState<string>(null);

    // If defaultOpen is true, the target cannot be found. To fix this, the render is conditional
    useEffect(() => {
        setTarget(aboutViewId);
    }, []);

    return (
        <div className={classNames(className)}>
            <Button id={aboutViewId} color="secondary" className="hub-btn" type="button">
                Info Hub
            </Button>

            {target && (
                <UncontrolledPopover
                    placement="bottom"
                    target={target}
                    trigger="legacy"
                    className="bs5 about-view-dropdown"
                    offset={[-215, 10]}
                    defaultOpen={defaultOpen}
                >
                    <PopoverBody className="p-1">
                        <AboutViewAnchored defaultOpen={defaultOpen ? "licensing" : null}>{children}</AboutViewAnchored>
                    </PopoverBody>
                </UncontrolledPopover>
            )}
        </div>
    );
};

interface AccordionItemWrapperProps {
    icon: IconName;
    color: TextColor;
    heading: string;
    description: string;
    children: ReactNode;
    pill?: boolean;
    pillText?: string;
    pillIcon?: IconName;
    targetId?: string;
}

const AccordionItemWrapper = (props: AccordionItemWrapperProps) => {
    const { icon, color, heading, description, children, pill, pillText, pillIcon } = props;
    const targetId = props.targetId ?? uniqueId();
    return (
        <AccordionItem className={classNames("rounded-3", `box-shadow-${color}`, "panel-bg-1")}>
            <AccordionHeader targetId={targetId}>
                <Icon icon={icon} color={color} className="tab-icon me-3" />
                <div className="vstack gap-1">
                    <div className="hstack flex-wrap gap-1">
                        <h4 className="m-0">{heading}</h4>
                        {pill && (
                            <Badge color="warning" pill className="text-uppercase accordion-pill">
                                <Icon icon={pillIcon} />
                                {pillText}
                            </Badge>
                        )}
                    </div>
                    <small className="description">{description}</small>
                </div>
            </AccordionHeader>
            <AccordionBody accordionId={targetId}>{children}</AccordionBody>
        </AccordionItem>
    );
};

const AboutViewAnchored = (props: Omit<AboutViewProps, "defaultOpen"> & { defaultOpen?: string | string[] }) => {
    const { children, className, defaultOpen } = props;

    todo("Feature", "Damian", "Once there is a new info hub view, consider changing defaultOpen");

    return (
        <div className={classNames(className)}>
            <UncontrolledAccordion flush stayOpen className="bs5 about-view-accordion" defaultOpen={defaultOpen}>
                {children}
            </UncontrolledAccordion>
        </div>
    );
};

interface AccordionItemLicensingProps {
    description: ReactNode;
    featureName: string;
    featureIcon: IconName;
    children: ReactNode;
    checkedLicenses: string[];
    isCommunityLimited?: boolean;
    isProfessionalLimited?: boolean;
}

const AccordionItemLicensing = (props: AccordionItemLicensingProps) => {
    const {
        featureName,
        featureIcon,
        children,
        checkedLicenses,
        description,
        isCommunityLimited,
        isProfessionalLimited,
    } = props;
    const licenses = [
        { name: "Community", checked: checkedLicenses.includes("Community") },
        { name: "Professional", checked: checkedLicenses.includes("Professional") },
        { name: "Enterprise", checked: checkedLicenses.includes("Enterprise") },
    ];
    return (
        <div className="text-center">
            <div className="lead mb-3 fs-4">{description}</div>
            <h4>
                <Icon icon={featureIcon} /> {featureName}
            </h4>
            <div className="d-flex flex-wrap gap-3 licensing-cols">
                {licenses.map((license) => (
                    <div className="vstack align-items-center" key={license.name}>
                        <h5 className={classNames("license-name", license.name.toLowerCase())}>{license.name}</h5>
                        <Icon icon={license.checked ? "tick" : "cancel"} />
                        {isCommunityLimited && license.name === "Community" ? (
                            <small className="text-muted">(limited)</small>
                        ) : null}
                        {isProfessionalLimited && license.name === "Professional" ? (
                            <small className="text-muted">(limited)</small>
                        ) : null}
                    </div>
                ))}
            </div>
            {children}
        </div>
    );
};

export default AboutViewFloating;
export {
    AboutViewFloating,
    AboutViewAnchored,
    AccordionItemLicensing,
    AccordionItemWrapper,
    AboutViewHeading,
    FeatureAvailabilityTable,
};
