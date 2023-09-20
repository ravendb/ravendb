import { Badge } from "reactstrap";
import React from "react";
import classNames from "classnames";

export type LicenseBadgeText = "Professional +" | "Enterprise";

interface LicenseRestrictedBadgeProps {
    className?: string;
    licenseRequired: LicenseBadgeText;
}

export default function LicenseRestrictedBadge({ className, licenseRequired }: LicenseRestrictedBadgeProps) {
    return (
        <Badge className={classNames("ms-2 license-restricted-badge", className, getClassName(licenseRequired))}>
            {licenseRequired}
        </Badge>
    );
}

function getClassName(licenseBadgeText: LicenseBadgeText) {
    switch (licenseBadgeText) {
        case "Enterprise":
            return "enterprise";
        case "Professional +":
            return "professional";
        default:
            return null;
    }
}
