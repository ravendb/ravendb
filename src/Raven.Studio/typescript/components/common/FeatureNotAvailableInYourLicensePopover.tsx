import React from "react";
import { UncontrolledPopover } from "reactstrap";
import { useRavenLink } from "hooks/useRavenLink";

interface FeatureNotAvailableInYourLicensePopoverProps {
    target: string;
}
export default function FeatureNotAvailableInYourLicensePopover(props: FeatureNotAvailableInYourLicensePopoverProps) {
    const { target } = props;
    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    return (
        <UncontrolledPopover trigger="hover" target={target} placement="top" className="bs5">
            <div className="p-3 text-center">
                Your current license does not support this feature.
                <br />
                <a href={upgradeLicenseLink} target="_blank">
                    Upgrade your plan
                </a>{" "}
                to access.
            </div>
        </UncontrolledPopover>
    );
}
