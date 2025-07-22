import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export default function AiAgentsInfoHub() {
    const hasAiAgent = useAppSelector(licenseSelectors.statusValue("HasAiAgent"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasAiAgent,
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
                TODO
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasAiAgent} data={featureAvailability} />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "AI Agents",
        featureIcon: "ai-agents",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: false },
        enterpriseAi: { value: true },
    },
];
