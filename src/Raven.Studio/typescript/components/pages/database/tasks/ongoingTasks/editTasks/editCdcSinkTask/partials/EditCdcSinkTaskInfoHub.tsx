import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export default function EditCdcSinkTaskInfoHub() {
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

    return (
        <AboutViewFloating defaultOpen={hasCdcSink ? null : "licensing"}>
            <AccordionItemWrapper
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
                targetId="about-view"
            >
                TODO
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasCdcSink} data={featureAvailability} />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "CDC Sink task",
        featureIcon: "sql-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
        enterpriseAi: { value: true },
    },
];
