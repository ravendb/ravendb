import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { featureAvailabilityProfessionalOrAbove } from "components/utils/licenseLimitsUtils";
import React from "react";

export function EditServerWideExternalReplicationInfoHub() {
    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove());

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
                    Defining a <strong>Server-Wide External-Replication task</strong> will create an ongoing
                    External-Replication task for each database in your cluster.
                    <ul className="margin-top-xs">
                        <li>You can select specific databases to exclude from the task.</li>
                        <li>
                            The configurations set in the Server-Wide task will be applied to the corresponding ongoing
                            task created per database.
                        </li>
                    </ul>
                </div>
                <div className="margin-top-sm">
                    The <strong>connection string</strong> used in each corresponding database task will be composed of:
                    <ul className="margin-top-xs">
                        <li>The discovery URLs provided here.</li>
                        <li>The target database name will be the individual source database name.</li>
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
