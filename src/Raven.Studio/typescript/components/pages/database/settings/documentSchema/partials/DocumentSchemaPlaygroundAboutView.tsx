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
            <AccordionItemWrapper icon="about" color="info">
                <div>
                    <ul>
                        <li>
                            In this playground, you can test how your documents validate against sample JSON schemas.
                        </li>
                        <li className="mt-2">
                            This is a safe, temporary workspace. No changes are made to your existing documents or to
                            your saved schema definitions.
                        </li>
                        <li className="mt-2">
                            Define one or more sample schemas per collection, run the test, and review any validation
                            errors found.
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
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
