import React, { ReactNode } from "react";
import "./AboutView.scss";
import {
    AccordionBody,
    AccordionHeader,
    AccordionItem,
    Badge,
    Button,
    PopoverBody,
    UncontrolledAccordion,
    UncontrolledPopover,
} from "reactstrap";
import classNames from "classnames";
import useId from "components/hooks/useId";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";

interface AboutViewProps {
    children?: ReactNode | ReactNode[];
    className?: string;
}

interface AboutViewHeadingProps {
    title: string;
    icon: IconName;
    badgeText?: string;
}

const AboutViewHeading = (props: AboutViewHeadingProps) => {
    const { title, icon, badgeText } = props;
    return (
        <h2 className="d-flex align-items-center gap-1 mb-5">
            <Icon icon={icon} /> {title}{" "}
            {badgeText != null && (
                <Badge color="faded-primary" className="about-view-title-badge">
                    {badgeText}
                </Badge>
            )}
        </h2>
    );
};

const AboutViewFloating = (props: AboutViewProps) => {
    const { children, className } = props;

    const popoverId = useId("aboutView");

    return (
        <div className={classNames(className)}>
            <Button color="secondary" id={popoverId} className="hub-btn">
                Info Hub
            </Button>

            <UncontrolledPopover
                placement="bottom"
                target={popoverId}
                trigger="legacy"
                className="bs5 about-view-dropdown"
                offset={[-215, 10]}
            >
                <UncontrolledAccordion flush stayOpen className="bs5 about-view-accordion">
                    <PopoverBody>{children}</PopoverBody>
                </UncontrolledAccordion>
            </UncontrolledPopover>
        </div>
    );
};

interface AccordionItemWrapperProps {
    targetId: string;
    icon: IconName;
    color: string;
    heading: string;
    description: string;
    children: ReactNode;
    pill?: boolean;
    pillText?: string;
    pillIcon?: IconName;
}

const AccordionItemWrapper = (props: AccordionItemWrapperProps) => {
    const { targetId, icon, color, heading, description, children, pill, pillText, pillIcon } = props;
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

const AboutViewAnchored = (props: AboutViewProps) => {
    const { children, className } = props;

    return (
        <div className={classNames(className)}>
            <UncontrolledAccordion flush stayOpen className="bs5 about-view-accordion">
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
}

const AccordionItemLicensing = (props: AccordionItemLicensingProps) => {
    const { featureName, featureIcon, children, checkedLicenses, description, isCommunityLimited } = props;
    const licenses = [
        { name: "Community", checked: checkedLicenses.includes("Community") },
        { name: "Professional", checked: checkedLicenses.includes("Professional") },
        { name: "Enterprise", checked: checkedLicenses.includes("Enterprise") },
    ];
    return (
        <div className="text-center">
            <p className="lead fs-4">{description}</p>
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
                    </div>
                ))}
            </div>
            {children}
        </div>
    );
};

export default AboutViewFloating;
export { AboutViewFloating, AboutViewAnchored, AccordionItemLicensing, AccordionItemWrapper, AboutViewHeading };
