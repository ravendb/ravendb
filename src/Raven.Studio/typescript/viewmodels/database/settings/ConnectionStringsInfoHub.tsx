import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { featureAvailabilityProfessionalOrAbove } from "components/utils/licenseLimitsUtils";
import React from "react";

export function ConnectionStringsInfoHub() {
    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove);

    return (
        <AboutViewFloating defaultOpen={isProfessionalOrAbove ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    <ul>
                        <li>
                            RavenDB is designed to interact with diverse data storage solutions via replication, ETL, or
                            incoming data processing.
                        </li>
                        <li className="margin-top-xxs">
                            From this view, you can manage all the connection strings that may be used when defining an
                            ongoing-task per data storage.
                        </li>
                        <li className="margin-top-xxs">
                            New connection strings that have been created within an ongoing-task view will also be
                            listed here.
                        </li>
                        <li className="margin-top-xxs">
                            Connection strings that are in use by ongoing-tasks cannot be deleted, as they are essential
                            for task functionality and data access.
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isProfessionalOrAbove}
                data={featureAvailabilityProfessionalOrAbove}
            />
        </AboutViewFloating>
    );
}
