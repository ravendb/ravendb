import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import React from "react";
import { useRavenLink } from "hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { getLicenseLimitReachStatus, useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

interface DatabaseCustomAnalyzersInfoHubProps {
    databaseAnalyzersCount: number;
}

export function DatabaseCustomAnalyzersInfoHub({ databaseAnalyzersCount }: DatabaseCustomAnalyzersInfoHubProps) {
    const customAnalyzersDocsLink = useRavenLink({ hash: "VWCQPI" });
    const licenseClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerCluster"));
    const licenseDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerDatabase"));
    const numberOfCustomAnalyzersInCluster = useAppSelector(licenseSelectors.limitsUsage).NumberOfAnalyzersInCluster;
    const hasServerWideCustomAnalyzers = useAppSelector(licenseSelectors.statusValue("HasServerWideAnalyzers"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: licenseDatabaseLimit,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: licenseClusterLimit,
            },
            {
                featureName: defaultFeatureAvailability[2].featureName,
                value: hasServerWideCustomAnalyzers,
            },
        ],
    });

    const databaseLimitReachStatus = getLicenseLimitReachStatus(databaseAnalyzersCount, licenseDatabaseLimit);
    const clusterLimitReachStatus = getLicenseLimitReachStatus(numberOfCustomAnalyzersInCluster, licenseClusterLimit);

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                targetId="1"
                icon="about"
                color="info"
                description="Get additional info on this feature"
                heading="About this view"
            >
                <p>
                    <strong>Analyzers</strong> are used by indexes to split the index-fields into tokens (terms).
                    <br />
                    The analyzer defines how the field is tokenized.
                    <br />
                    When querying an index, these terms are used to define the search criteria and filter query results.
                </p>
                <div>
                    <strong>In this view</strong>, you can add your own analyzers in addition to the existing analyzers
                    that come with RavenDB.
                    <ul>
                        <li>The custom analyzers added here can be used only by indexes in this database.</li>
                        <li>The server-wide custom analyzers listed can also be used in this database.</li>
                        <li>Note: custom analyzers are not supported by Corax indexes.</li>
                    </ul>
                </div>
                <div>
                    Provide <code>C#</code> code in the editor view, or upload from file.
                    <ul>
                        <li>The analyzer name must be the same as the analyzer&apos;s class name in your code.</li>
                        <li>
                            Inherit from <code>Lucene.Net.Analysis.Analyzer</code>
                        </li>
                        <li>
                            Code must be compilable and include all necessary <code>using</code> statements.
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={customAnalyzersDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Custom Analyzers
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={
                    databaseLimitReachStatus === "notReached" &&
                    clusterLimitReachStatus === "notReached" &&
                    hasServerWideCustomAnalyzers
                }
                data={featureAvailability}
            />
        </AboutViewAnchored>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Limit per database",
        community: { value: 1 },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
    {
        featureName: "Limit per cluster",
        community: { value: 5 },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
    {
        featureName: "Server-wide custom analyzers",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
