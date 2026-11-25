import Badge from "react-bootstrap/Badge";
import React from "react";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "./shell/licenseSlice";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useCloudLicenseMessage } from "hooks/useCloudLicenseMessage";
import type { CloudOptionalFeature } from "components/common/FeatureAvailabilitySummary";

export type LicenseBadgeText = "Professional +" | "Enterprise" | "Enterprise AI";

interface LicenseRestrictedBadgeProps {
    className?: string;
    licenseRequired: LicenseBadgeText;
    cloudOptionalFeature?: CloudOptionalFeature;
}

export default function LicenseRestrictedBadge({
    className,
    licenseRequired,
    cloudOptionalFeature,
}: LicenseRestrictedBadgeProps) {
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const cloudLicenseMessage = useCloudLicenseMessage(cloudOptionalFeature);

    const iconName = getIconName(licenseRequired, isCloud);
    const popoverMessage = cloudLicenseMessage ?? getPopoverMessage(licenseRequired);

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
    if (isCloud || licenseBadgeText === "Enterprise" || licenseBadgeText === "Enterprise AI") {
        return "use-cases";
    }

    return "building";
}

function getPopoverMessage(licenseBadgeText: LicenseBadgeText): string {
    switch (licenseBadgeText) {
        case "Professional +":
            return "Available from Professional license and above";
        case "Enterprise":
            return "Available in Enterprise license";
        case "Enterprise AI":
            return "Available in RavenDB AI license";
        default:
            return "";
    }
}
