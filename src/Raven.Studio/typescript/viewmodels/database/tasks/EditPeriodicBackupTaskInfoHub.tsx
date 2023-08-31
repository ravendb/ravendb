import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import AccordionCommunityLicenseNotIncluded from "components/common/AccordionCommunityLicenseNotIncluded";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";

export function EditPeriodicBackupTaskInfoHub() {
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
                    This is periodic backup task edit view
                </p>
            </AccordionItemWrapper>
            {licenseType === "Community" && (
                <AccordionCommunityLicenseNotIncluded
                    targetId="licensing"
                    featureName="Periodic Backups"
                    featureIcon="periodic-backup"
                />
            )}
        </AboutViewFloating>
    );
}
