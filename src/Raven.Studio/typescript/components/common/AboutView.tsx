import "./AboutView.scss";
import { ReactNode } from "react";
import Badge from "react-bootstrap/Badge";
import { AccordionBody, AccordionHeader, AccordionItem, UncontrolledAccordion } from "reactstrap";
import Button from "react-bootstrap/Button";
import classNames from "classnames";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";
import { TextColor } from "components/models/common";
import { uniqueId } from "lodash";
import LicenseRestrictedBadge, { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import { OverlayTrigger, Popover } from "react-bootstrap";

interface AboutViewProps {
    children?: ReactNode | ReactNode[];
    className?: string;
    defaultOpen?: "licensing" | (string & NonNullable<unknown>) | string[];
}

interface AboutViewHeadingProps {
    title: string;
    icon: IconName;
    licenseBadgeText?: LicenseBadgeText;
    marginBottom?: number;
}

const AboutViewHeading = (props: AboutViewHeadingProps) => {
    const { title, icon, licenseBadgeText, marginBottom } = props;

    return (
        <h2 className={classNames("d-flex align-items-center gap-1 flex-wrap", `mb-${marginBottom ?? 5}`)}>
            <Icon icon={icon} /> {title}{" "}
            {licenseBadgeText != null && <LicenseRestrictedBadge licenseRequired={licenseBadgeText} />}
        </h2>
    );
};

const aboutViewId = "about-view";

const AboutViewFloating = (props: AboutViewProps) => {
    const { children, className, defaultOpen } = props;

    return (
        <div className={classNames(className)}>
            <OverlayTrigger
                defaultShow={!!defaultOpen}
                placement="bottom"
                trigger="click"
                popperConfig={{
                    modifiers: [
                        {
                            name: "offset",
                            options: {
                                offset: [-140, 10],
                            },
                        },
                    ],
                }}
                overlay={
                    <Popover id={aboutViewId} className="bs5 about-view-dropdown" style={{ width: "100%" }}>
                        <Popover.Body className="p-1">
                            <AboutViewAnchored defaultOpen={defaultOpen ? "licensing" : null}>
                                {children}
                            </AboutViewAnchored>
                        </Popover.Body>
                    </Popover>
                }
            >
                <Button id={aboutViewId} variant="secondary" className="hub-btn" type="button">
                    Info Hub
                </Button>
            </OverlayTrigger>
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
    className?: string;
}

const AccordionItemWrapper = (props: AccordionItemWrapperProps) => {
    const { icon, color, heading, description, children, pill, pillText, pillIcon, className } = props;
    const targetId = props.targetId ?? uniqueId();
    return (
        <AccordionItem className={classNames("license-accordion", `box-shadow-${color}`, "panel-bg-1", className)}>
            <AccordionHeader targetId={targetId}>
                <Icon icon={icon} color={color} className="tab-icon me-3" />
                <div className="vstack gap-1">
                    <div className="hstack flex-wrap gap-1">
                        <h4 className="m-0">{heading}</h4>
                        {pill && (
                            <Badge bg="warning" pill className="text-uppercase accordion-pill">
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

    // UncontrolledAccordion works incorrectly if we do not provide an array
    const defaultOpen = Array.isArray(props.defaultOpen) ? props.defaultOpen : [props.defaultOpen];

    return (
        <div className={classNames(className)}>
            <UncontrolledAccordion
                className="bs5 about-view-accordion"
                flush
                stayOpen
                defaultOpen={defaultOpen}
                // reactstrap make it required in 9.2.1 but it is not used. Probably will be removed in new version
                toggle={null}
            >
                {children}
            </UncontrolledAccordion>
        </div>
    );
};

export default AboutViewFloating;
export { AboutViewFloating, AboutViewAnchored, AccordionItemWrapper, AboutViewHeading };
