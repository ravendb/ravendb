import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";

export default function IndexCleanupAboutView() {
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasIndexCleanup,
            },
        ],
    });

    return (
        <AboutViewFloating defaultOpen={hasIndexCleanup ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    Maintaining multiple indexes can lower performance. Every time data is inserted, updated, or
                    deleted, the corresponding indexes need to be updated as well, which can lead to increased write
                    latency.
                </p>
                <p className="mb-0">
                    To counter these performance issues, RavenDB recommends a set of actions to optimize the number of
                    indexes. Note that when merging or removing indexes, you need to update the index reference in your
                    application.
                </p>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasIndexCleanup} data={featureAvailability} />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Index Cleanup",
        featureIcon: "index-cleanup",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
