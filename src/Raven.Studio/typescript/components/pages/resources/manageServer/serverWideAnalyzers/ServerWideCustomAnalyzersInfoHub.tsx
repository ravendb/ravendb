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

export default function ServerWideCustomAnalyzersInfoHub() {
    const customAnalyzersDocsLink = useRavenLink({ hash: "VWCQPI" });

    const hasServerWideCustomAnalyzers = useAppSelector(licenseSelectors.statusValue("HasServerWideAnalyzers"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasServerWideCustomAnalyzers,
            },
        ],
    });

    return (
        <AboutViewAnchored defaultOpen={hasServerWideCustomAnalyzers ? null : "licensing"}>
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
                        <li>
                            The custom analyzers added here can be used by indexes in ALL databases in your cluster.
                        </li>
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
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasServerWideCustomAnalyzers} data={featureAvailability} />
        </AboutViewAnchored>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Server-Wide Custom Analyzers",
        featureIcon: "server-wide-custom-analyzers",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
