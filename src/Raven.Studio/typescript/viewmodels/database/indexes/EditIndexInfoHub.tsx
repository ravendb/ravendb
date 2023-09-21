import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";
import {Icon} from "components/common/Icon";
import {useRavenLink} from "hooks/useRavenLink";

export function EditIndexInfoHub() {
    const hasAdditionalAssembliesFromNuGet = useAppSelector(licenseSelectors.statusValue("HasAdditionalAssembliesFromNuGet"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasAdditionalAssembliesFromNuGet,
            },
        ],
    });
    const indexViewDocsLink = useRavenLink({ hash: "XZXJP2" });
    
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
                    Create a new index or edit an existing one in this view.
                    <br/>
                    The indexing process will immediately start upon saving the index.
                </p>
                <ul>
                    <li>
                        Various index types can be created:
                        <br />
                        Map / Multi-Map / Map-Reduce / Multi-Map-Reduce
                    </li>
                    <li className="margin-top-xxs">
                        Select a deployment mode for the index:
                        <br />
                        Parallel / Rolling
                    </li>
                    <li className="margin-top-xxs">
                        You can configure the index fields for:
                        <br />
                        Full-text search, Highlighting, Suggestions, Spatial queries,
                        <br /> 
                        and Store index fields.
                    </li>
                    <li className="margin-top-xxs">
                        Customize the index configuration:
                        <br />
                        e.g. select indexing engine: Corax / Lucene
                    </li>
                    <li className="margin-top-xxs">
                        Add additional assemblies and sources to enhance your index functions by referencing classes and
                        methods from other files.
                    </li>
                    <li className="margin-top-xxs">
                        The index can be tested in this view before saving it.
                    </li>
                    <li className="margin-top-xxs">
                        Index history is available when editing an existing index,
                        <br />
                        showing the latest index revisions.
                    </li>
                </ul>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={indexViewDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Index View
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasAdditionalAssembliesFromNuGet} data={featureAvailability} />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Additional Assemblies from NuGet",
        featureIcon: "additional-assemblies",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
