import { Badge } from "reactstrap";
import React from "react";
import classNames from "classnames";

interface LicenseRestrictedBadgeProps {
    className?: string;
    licenseRequired: string;
}

export default function LicenseRestrictedBadge({ className, licenseRequired }: LicenseRestrictedBadgeProps) {
    return (
        <Badge color="faded-primary" className={classNames("ms-2 license-restricted-badge", className)}>
            {licenseRequired}
        </Badge>
    );
}
