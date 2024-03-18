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
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { NonShardedViewProps } from "components/models/common";

export function DatabaseCustomSortersInfoHub({ db }: NonShardedViewProps) {
    const { databasesService } = useServices();
    const asyncGetDatabaseSorters = useAsync(() => databasesService.getCustomSorters(db), [db]);
    const customSortersDocsLink = useRavenLink({ hash: "XI6BMT" });
    const licenseClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomSortersPerCluster"));
    const licenseDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomSortersPerDatabase"));
    const numberOfCustomSortersInCluster = useAppSelector(licenseSelectors.limitsUsage).NumberOfCustomSortersInCluster;
    const hasServerWideCustomSorters = useAppSelector(licenseSelectors.statusValue("HasServerWideCustomSorters"));

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
                value: hasServerWideCustomSorters,
            },
        ],
    });

    const databaseResultsCount = asyncGetDatabaseSorters.result?.length ?? null;

    const databaseLimitReachStatus = getLicenseLimitReachStatus(databaseResultsCount, licenseDatabaseLimit);
    const clusterLimitReachStatus = getLicenseLimitReachStatus(numberOfCustomSortersInCluster, licenseClusterLimit);

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
                    A <strong>Custom Sorter</strong> allows you to define how documents will be ordered in the query
                    results
                    <br /> according to your specific requirements.
                </p>
                <div>
                    <strong>In this view</strong>, you can add your own sorters:
                    <ul className="margin-top-xxs">
                        <li>The custom sorters added here can be used only with queries in this database.</li>
                        <li>The server-wide custom sorters listed can also be applied within this database.</li>
                        <li>The custom sorters can be tested in this view with a sample query.</li>
                        <li>Note: custom sorters are not supported when querying Corax indexes.</li>
                    </ul>
                </div>
                <div>
                    Provide <code>C#</code> code in the editor view, or upload from file:
                    <ul className="margin-top-xxs">
                        <li>The sorter name must be the same as the sorter&apos;s class name in your code.</li>
                        <li>
                            Inherit from <code>Lucene.Net.Search.FieldComparator</code>
                        </li>
                        <li>
                            Code must be compilable and include all necessary <code>using</code> statements.
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={customSortersDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Custom Sorters
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={
                    databaseLimitReachStatus === "notReached" &&
                    clusterLimitReachStatus === "notReached" &&
                    hasServerWideCustomSorters
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
        featureName: "Server-wide custom sorters",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
