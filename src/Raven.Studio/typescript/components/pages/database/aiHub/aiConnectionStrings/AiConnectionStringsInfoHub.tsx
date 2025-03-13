import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "hooks/useAppUrls";

export function AiConnectionStringsInfoHub() {
    const hasEmbeddingsGeneration = useAppSelector(licenseSelectors.statusValue("HasEmbeddingsGeneration"));

    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasEmbeddingsGeneration,
            },
        ],
    });

    return (
        <AboutViewAnchored defaultOpen={hasEmbeddingsGeneration ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    <ul>
                        <li>
                            This view allows you to define and manage connection strings for AI model services that{" "}
                            <strong>generate embeddings from text</strong>.
                        </li>
                        <li className="mt-1">
                            Each connection string specifies the details required to connect to a particular provider
                            and can be reused across multiple{" "}
                            <a href={appUrl.forAiTasks(activeDatabaseName)} target="_blank">
                                embedding generation tasks
                            </a>{" "}
                            in the database.
                        </li>
                        <li className="mt-1">
                            Supported providers include:
                            <ul>
                                <li>Azure OpenAI</li>
                                <li>Google AI</li>
                                <li>Hugging Face</li>
                                <li>Ollama</li>
                                <li>OpenAI</li>
                                <li>Mistral AI</li>
                                <li>The built-in local model (bge-micro-v2)</li>
                            </ul>
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasEmbeddingsGeneration} data={featureAvailability} />
        </AboutViewAnchored>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "AI",
        featureIcon: "ai-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
