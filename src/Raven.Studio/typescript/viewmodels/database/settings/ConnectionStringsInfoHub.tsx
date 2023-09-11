import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import AccordionLicenseNotIncluded from "components/common/AccordionLicenseNotIncluded";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";

export function ConnectionStringsInfoHub() {
    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove());

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    Text
                </p>
            </AccordionItemWrapper>
            <AccordionLicenseNotIncluded
                targetId="licensing"
                featureName="Connection Strings"
                featureIcon="manage-connection-strings"
                checkedLicenses={["Professional", "Enterprise"]}
                isLimited={!isProfessionalOrAbove}
            />
        </AboutViewFloating>
    );
}
