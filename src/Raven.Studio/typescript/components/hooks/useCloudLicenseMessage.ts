import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { CloudOptionalFeature } from "components/common/FeatureAvailabilitySummary";

export function useCloudLicenseMessage(cloudOptionalFeature?: CloudOptionalFeature): string | null {
    const t = useStudioTranslation("featureAvailabilitySummary");
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const licenseType = useAppSelector(licenseSelectors.licenseType);

    if (!isCloud) {
        return null;
    }

    if (licenseType === "Community") {
        return t("cloudProductionPlanOnly");
    }

    if (cloudOptionalFeature) {
        return t("cloudFeatureNotTurnedOnForFeature", {
            feature: t("cloudFeatureName", { context: cloudOptionalFeature }),
        });
    }

    return t("cloudFeatureNotTurnedOn");
}
