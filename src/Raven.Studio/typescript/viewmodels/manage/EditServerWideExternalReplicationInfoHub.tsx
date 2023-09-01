import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import AccordionLicenseNotIncluded from "components/common/AccordionLicenseNotIncluded";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";

export function EditServerWideExternalReplicationInfoHub() {
    const licenseType = useAppSelector(licenseSelectors.licenseType);

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
                    This is server-wide external replication task edit view
                </p>
            </AccordionItemWrapper>
            {licenseType === "Community" && (
                <AccordionLicenseNotIncluded
                    targetId="licensing"
                    featureName="Server-Wide External Replication"
                    featureIcon="server-wide-replication"
                    checkedLicenses={["Professional", "Enterprise"]}
                />
            )}
        </AboutViewFloating>
    );
}
