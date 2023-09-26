import { Badge } from "reactstrap";
import React from "react";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "./shell/licenseSlice";

export type LicenseBadgeText = "Professional +" | "Enterprise";

interface LicenseRestrictedBadgeProps {
    className?: string;
    licenseRequired: LicenseBadgeText;
}

export default function LicenseRestrictedBadge({ className, licenseRequired }: LicenseRestrictedBadgeProps) {
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    return (
        <Badge
            className={classNames("ms-2 license-restricted-badge", className, getClassName(licenseRequired, isCloud))}
        >
            {isCloud ? "Production" : licenseRequired}
        </Badge>
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
