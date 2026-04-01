import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export function EditCdcSinkTaskInfoHub() {
    const hasCdcSink = useAppSelector(licenseSelectors.statusValue("HasCdcSink"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasCdcSink,
            },
        ],
    });

    const cdcSinkDocsLink = useRavenLink({ hash: "CDC_SINK" });

    return (
        <AboutViewFloating defaultOpen={hasCdcSink ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    <strong>CDC Sink</strong> allows you to capture changes from an external SQL database
                    using Change Data Capture (CDC) and replicate them into RavenDB documents.
                </p>
                <p>
                    Tables from the source database are mapped to RavenDB collections. Column values
                    become document properties, and you can customize the mapping or add transformation
                    patches.
                </p>
                <hr />
                <div>
                    <a href={cdcSinkDocsLink} target="_blank">
                        <Icon icon="newtab" /> CDC Sink documentation
                    </a>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasCdcSink}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "CDC Sink",
        featureIcon: "sql-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
