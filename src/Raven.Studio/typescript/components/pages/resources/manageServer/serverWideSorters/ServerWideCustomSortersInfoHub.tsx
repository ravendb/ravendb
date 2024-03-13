import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { Icon } from "components/common/Icon";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";

export default function ServerWideCustomSortersInfoHub() {
    const customSortersDocsLink = useRavenLink({ hash: "LGUJH8" });

    const hasServerWideCustomSorters = useAppSelector(licenseSelectors.statusValue("HasServerWideCustomSorters"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasServerWideCustomSorters,
            },
        ],
    });

    return (
        <AboutViewAnchored defaultOpen={hasServerWideCustomSorters ? null : "licensing"}>
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
                    <br />
                    according to your specific requirements.
                </p>
                <div>
                    <strong>In this view</strong>, you can add your own sorters:
                    <ul className="margin-top-xxs">
                        <li>
                            The custom sorters added here can be used with queries in ALL databases in your cluster.
                        </li>
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
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasServerWideCustomSorters} data={featureAvailability} />
        </AboutViewAnchored>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Server-Wide Custom Sorters",
        featureIcon: "server-wide-custom-sorters",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
