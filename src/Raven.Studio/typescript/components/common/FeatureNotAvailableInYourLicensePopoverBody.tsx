import { useRavenLink } from "hooks/useRavenLink";
import { useCloudLicenseMessage } from "hooks/useCloudLicenseMessage";
import type { CloudOptionalFeature } from "components/common/FeatureAvailabilitySummary";

interface FeatureNotAvailableInYourLicensePopoverBodyProps {
    cloudOptionalFeature?: CloudOptionalFeature;
}

export default function FeatureNotAvailableInYourLicensePopoverBody({
    cloudOptionalFeature,
}: FeatureNotAvailableInYourLicensePopoverBodyProps) {
    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });
    const cloudLicenseMessage = useCloudLicenseMessage(cloudOptionalFeature);

    if (cloudLicenseMessage) {
        return <div className="text-center">{cloudLicenseMessage}</div>;
    }

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
