import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "hooks/useAppUrls";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export default function EditGenAiTaskInfoHub() {
    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const hasGenAi = useAppSelector(licenseSelectors.statusValue("HasGenAi"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasGenAi,
            },
        ],
    });

    return (
        <AboutViewFloating defaultOpen={hasGenAi ? null : "licensing"}>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <div>
                    <p>
                        You can configure a <strong>GenAI task</strong> to analyze and enrich your documents using{" "}
                        <strong>LLMs</strong>.
                        <br />
                        The task can automatically update existing documents or create new ones based on AI-generated
                        content.
                    </p>
                    <hr />
                    Task configuration includes:
                    <br />
                    <br />
                    <p>
                        <strong>AI service and model:</strong>
                        <br />
                        Select a{" "}
                        <a href={appUrl.forAiConnectionStrings(activeDatabaseName)} target="_blank">
                            connection string{" "}
                        </a>{" "}
                        that defines the AI provider and model to connect to.
                    </p>
                    <div>
                        <strong>Define MODEL INPUT:</strong>
                        <ul>
                            <li className="mt-1">
                                <strong>Context objects</strong>
                                <br /> Provide a script to create context objects from each document in the source
                                collection. These will be the input objects for the model.
                            </li>
                            <li className="mt-1">
                                <strong>Prompt</strong>
                                <br /> Define the instruction sent to the model.
                                <br /> It will be applied to each context object.
                            </li>
                            <li className="mt-1">
                                <strong>JSON schema</strong>
                                <br /> Specify the expected format of the output objects the model should generate.
                                <br /> An output object will be returned for each input context object.
                            </li>
                        </ul>
                    </div>
                    <p>
                        <strong>Handle MODEL OUTPUT:</strong>
                        <br /> Provide an &quot;update script&quot; that will process each output object returned by the
                        model.
                        <br /> You can modify existing documents or create new ones based on the generated results, as
                        needed.
                    </p>
                </div>
                <hr />
                <p>
                    <strong>Testing:</strong>
                    <br />
                    You can test the task definition at any stage using any source document.
                    <br /> Use the playground to provide custom context objects and/or model output,
                    <br /> and preview the full flow before saving the task.
                </p>
                <p>
                    <strong>Ongoing processing:</strong>
                    <br /> Once the task is active, any modification to a document in the selected collection
                    <br /> will trigger the task to retrieve content from the model and apply the update script.
                </p>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasGenAi} data={featureAvailability} />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "GenAI",
        featureIcon: "genai",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: false },
        enterpriseAi: { value: true },
    },
];
