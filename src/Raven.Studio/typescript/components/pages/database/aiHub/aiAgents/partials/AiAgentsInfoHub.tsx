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
        <AboutViewFloating defaultOpen={hasAiAgent ? null : "licensing"}>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <div>
                    <ul>
                        <li>
                            An AI Agent is a natural language <strong>conversational assistant</strong> powered by an
                            LLM.
                        </li>
                        <li className="mt-1">
                            The agent enables conversations with an LLM about your data. It can retrieve information
                            from your database to answer prompts and trigger specific actions when needed.
                        </li>
                        <li className="mt-1">
                            This view displays all defined AI Agents.
                            <br /> You can add new agents, edit, delete, or clone an existing one as a starting point.
                        </li>
                        <li className="mt-1">
                            To interact with an agent,
                            <br /> click &quot;Start new chat&quot; and begin a conversation directly from the Studio.
                        </li>
                    </ul>
                </div>
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
