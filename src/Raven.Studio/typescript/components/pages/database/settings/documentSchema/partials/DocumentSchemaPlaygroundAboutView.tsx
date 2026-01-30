import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export default function DocumentSchemaPlaygroundAboutView() {
    const hasSchemaValidation = useAppSelector(licenseSelectors.statusValue("HasSchemaValidation"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasSchemaValidation,
            },
        ],
    });

    return (
        <AboutViewAnchored>
            {/*TODO: remove if about view is finished*/}
            {false && (
                <AccordionItemWrapper
                    icon="about"
                    color="info"
                    description="Get additional info on this feature"
                    heading="About this view"
                >
                    todo
                </AccordionItemWrapper>
            )}
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasSchemaValidation} data={featureAvailability} />
        </AboutViewAnchored>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Schema Validation",
        featureIcon: "document-schema",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
