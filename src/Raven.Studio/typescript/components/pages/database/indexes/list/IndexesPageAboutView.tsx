import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { Icon } from "components/common/Icon";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";

interface IndexesPageAboutViewProps {
    isUnlimited: boolean;
}

export default function IndexesPageAboutView({ isUnlimited }: IndexesPageAboutViewProps) {
    const overviewDocsLink = useRavenLink({ hash: "8VWNHJ" });
    const listDocsLink = useRavenLink({ hash: "7HOOEA" });

    const autoClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfAutoIndexesPerCluster"));
    const staticClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfStaticIndexesPerCluster"));
    const autoDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfAutoIndexesPerDatabase"));
    const staticDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfStaticIndexesPerDatabase"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: autoClusterLimit,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: staticClusterLimit,
            },
            {
                featureName: defaultFeatureAvailability[2].featureName,
                value: autoDatabaseLimit,
            },
            {
                featureName: defaultFeatureAvailability[3].featureName,
                value: staticDatabaseLimit,
            },
        ],
    });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
                targetId="about-view"
            >
                <p>
                    Manage all indexes in the database from this view.
                    <br />
                    The indexes are grouped based on their associated collections.
                </p>
                <ul>
                    <li>
                        <strong>Detailed information</strong> for each index is provided such as:
                        <br />
                        the index type and data source, its current state, staleness status, the number of
                        index-entries, etc.
                    </li>
                    <li className="margin-top-xs">
                        <strong>Various actions</strong> can be performed such as:
                        <br />
                        create a new index, modify existing, delete, restart, disable or pause indexing, set index
                        priority, and more.
                    </li>
                </ul>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={overviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Indexes Overview
                </a>
                <br />
                <a href={listDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Indexes List View
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={isUnlimited} data={featureAvailability} />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Auto indexes limit per cluster",
        community: { value: Infinity },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
    {
        featureName: "Static indexes limit per cluster",
        community: { value: Infinity },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
    {
        featureName: "Auto indexes limit per database",
        community: { value: Infinity },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
    {
        featureName: "Static indexes limit per database",
        community: { value: Infinity },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
];
