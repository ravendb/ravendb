import Badge from "react-bootstrap/Badge";
import React from "react";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "./shell/licenseSlice";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

export type LicenseBadgeText = "Professional +" | "Enterprise";

interface LicenseRestrictedBadgeProps {
    className?: string;
    licenseRequired: LicenseBadgeText;
}

export default function LicenseRestrictedBadge({ className, licenseRequired }: LicenseRestrictedBadgeProps) {
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const iconName = getIconName(licenseRequired, isCloud);
    const popoverMessage = getPopoverMessage(licenseRequired, isCloud);

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
                <Icon icon={iconName} margin="m-0" />
                {!isCloud && licenseRequired === "Professional +" && "+"}
            </Badge>
        </PopoverWithHoverWrapper>
    );
}

function getClassName(licenseBadgeText: LicenseBadgeText, isCloud: boolean): "enterprise" | "professional" {
    if (isCloud) {
        return "enterprise";
    }

    switch (licenseBadgeText) {
        case "Enterprise":
            return "enterprise";
        case "Professional +":
            return "professional";
        default:
            return null;
    }
}

function getIconName(licenseBadgeText: LicenseBadgeText, isCloud: boolean): IconName {
    if (isCloud || licenseBadgeText === "Enterprise") {
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
        default:
            return "";
    }
}
