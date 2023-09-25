import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {FeatureAvailabilityData} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";

export function IntegrationsInfoHub() {
    const hasPostgreSql = useAppSelector(licenseSelectors.statusValue("HasPostgreSqlIntegration"));
    const hasPowerBi = useAppSelector(licenseSelectors.statusValue("HasPowerBI"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasPostgreSql,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: hasPowerBi,
            }
        ],
    });

    const hasAllFeaturesInLicense = hasPostgreSql && hasPowerBi;

    return (
        <AboutViewFloating defaultOpen={hasAllFeaturesInLicense ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    Text
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasAllFeaturesInLicense}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "PostgreSQL",
        featureIcon: "postgresql",
        community: { value: true },
        professional: { value: true },
        enterprise: { value: true },
    },{
        featureName: "Power BI",
        featureIcon: "powerbi",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    }
];
