import Badge from "react-bootstrap/Badge";
import React from "react";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "./shell/licenseSlice";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

export type LicenseBadgeText = "Professional +" | "Enterprise" | "Enterprise AI";

interface LicenseRestrictedBadgeProps {
    className?: string;
    licenseRequired: LicenseBadgeText;
}

export default function LicenseRestrictedBadge({ className, licenseRequired }: LicenseRestrictedBadgeProps) {
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const iconName = getIconName(licenseRequired, isCloud);
    const popoverMessage = getPopoverMessage(licenseRequired, isCloud);

    // the AI tier is drawn with the shared gradient rather than a flat colour, the same way
    // FeatureAvailabilitySummary and the chatbot icons mark it - on cloud every tier reads
    // as Production, so the gradient would misrepresent it there
    const isAiGradient = !isCloud && licenseRequired === "Enterprise AI";

    return (
        <PopoverWithHoverWrapper
            message={popoverMessage}
            placement="top"
            wrapperClassName={classNames("license-restricted-badge ms-2", className)}
        >
            <Badge
                data-testid="license-restricted-badge"
                className={classNames("license-restricted-badge", getClassName(licenseRequired, isCloud))}
                bg="secondary"
            >
                <Icon icon={iconName} margin="m-0" className={classNames({ "ai-gradient": isAiGradient })} />
                {!isCloud && licenseRequired === "Professional +" && "+"}
            </Badge>
        </PopoverWithHoverWrapper>
    );
}

type LicenseClassName = "enterprise" | "professional" | "enterprise-ai";

function getClassName(licenseBadgeText: LicenseBadgeText, isCloud: boolean): LicenseClassName {
    if (isCloud) {
        return "enterprise";
    }

    switch (licenseBadgeText) {
        case "Enterprise":
            return "enterprise";
        case "Professional +":
            return "professional";
        case "Enterprise AI":
            return "enterprise-ai";
        default:
            return null;
    }
}

function getIconName(licenseBadgeText: LicenseBadgeText, isCloud: boolean): IconName {
    // Enterprise AI shares the Enterprise glyph - its own colour class is what tells them apart
    if (isCloud || licenseBadgeText === "Enterprise" || licenseBadgeText === "Enterprise AI") {
        return "use-cases";
    }

    return "building";
}

function getPopoverMessage(licenseBadgeText: LicenseBadgeText, isCloud: boolean): string {
    if (isCloud) {
        return "Available in the Production plan";
    }

    switch (licenseBadgeText) {
        case "Professional +":
            return "Available from Professional license and above";
        case "Enterprise":
            return "Available in Enterprise license";
        case "Enterprise AI":
            // licenseModel.licenseTypeTextProvider calls this tier "RavenDB AI" to the user
            return "Available in RavenDB AI license";
        default:
            return "";
    }
}
