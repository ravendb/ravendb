import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {FeatureAvailabilityData} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";

export function CertificatesInfoHub() {
    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove);
    const featureAvailabilityData: FeatureAvailabilityData[] = [
        {
            featureName: "Read-Only Certificates",
            featureIcon: "access-read",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
        }
    ];


    return (
        <AboutViewFloating>
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
                isUnlimited={isProfessionalOrAbove}
                data={featureAvailabilityData}
            />
        </AboutViewFloating>
    );
}
