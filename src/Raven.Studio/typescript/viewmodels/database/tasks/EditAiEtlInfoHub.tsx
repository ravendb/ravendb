import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import FeatureAvailabilitySummaryWrapper, { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export function EditAiEtlInfoHub() {
    const hasAiEtl = useAppSelector(licenseSelectors.statusValue("HasAiEtl"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasAiEtl,
            },
        ],
    });
    
    const docsLink = "#"; // TODO

    return (
        <AboutViewFloating defaultOpen={hasAiEtl ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                TODO
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={docsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - AI ETL
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasAiEtl}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "AI ETL",
        featureIcon: "question", // TODO kalczur icon
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
