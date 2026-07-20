import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "hooks/useAppUrls";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";

export function AiConnectionStringsInfoHub() {
    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const aiConnectionStringsDocsLink = useRavenLink({ hash: "EAIKIA" });

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <ul>
                        <li>This view allows you to define and manage connection strings for AI model services.</li>
                        <li className="mt-1">
                            Each connection string specifies the details required to connect to a particular provider
                            and can be reused across multiple{" "}
                            <a href={appUrl.forAiTasks(activeDatabaseName)} target="_blank">
                                AI tasks
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
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={aiConnectionStringsDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - AI Connection Strings
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={true} data={defaultFeatureAvailability} />
        </AboutViewAnchored>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "AI",
        featureIcon: "ai-etl",
        community: { value: true },
        professional: { value: true },
        enterprise: { value: true },
    },
];
