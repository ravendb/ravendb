import { useRavenLink } from "hooks/useRavenLink";

export default function FeatureNotAvailableInYourLicensePopoverBody() {
    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    return (
        <div className="text-center">
            Your current license does not support this feature.
            <br />
            <a href={upgradeLicenseLink} target="_blank">
                Upgrade your plan
            </a>{" "}
            to access.
        </div>
    );
}
