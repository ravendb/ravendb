import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import FeatureAvailabilitySummaryWrapper, { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useAppUrls } from "hooks/useAppUrls";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { allAiExternalProviders } from "components/utils/common";

export function EditEmbeddingsGenerationInfoHub() {
    const hasEmbeddingsGeneration = useAppSelector(licenseSelectors.statusValue("HasEmbeddingsGeneration"));

    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: hasEmbeddingsGeneration,
            },
        ],
    });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    <p>
                        In this view, you can configure an ongoing task to generate embeddings from your documents&apos;
                        content using an AI service.
                        <br />
                        The generated embeddings are saved in a dedicated collection and can be used for vector search.
                    </p>
                    <hr />
                    Configuration includes:
                    <br />
                    <br />
                    <p>
                        <strong>Destination AI service:</strong>
                        <br />
                        Select a{" "}
                        <a href={appUrl.forAiConnectionStrings(activeDatabaseName)} target="_blank">
                            connection string{" "}
                        </a>{" "}
                        to specify which AI service and model to use.
                    </p>
                    <p>
                        <strong>Source data:</strong>
                        <br />
                        Select the source collection. Extract text directly from document properties or transform it
                        using a script. Define the chunking method for splitting the content.
                    </p>
                    <p>
                        <strong>Quantization method:</strong>
                        <br />
                        Choose the format for saving the generated embeddings.
                    </p>
                    <p>
                        <strong>Embeddings expiration:</strong>
                        <br />
                        Set how long the generated embeddings are retained in the database.
                    </p>
                    <p>
                        <strong>Query-time settings:</strong>
                        <br />
                        Embeddings are also generated for search terms used in vector search queries. Configure chunking
                        and cache expiration for these embeddings.
                    </p>
                </div>
                <hr />
                <div>
                    <strong>Ongoing processing:</strong>
                    <br />
                    Whenever a document in the selected collection is modified, the task is triggered to extract the
                    updated text and regenerate its embeddings.
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasEmbeddingsGeneration}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Embedded Model",
        featureIcon: "ai-etl",
        community: { value: true },
        professional: { value: true },
        enterprise: { value: true },
        helperInfo: "bge-micro-v2",
    },
    {
        featureName: "External Models",
        featureIcon: "ai-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
        helperInfo: (
            <ul>
                {allAiExternalProviders.map((provider) => (
                    <li key={provider}>{provider}</li>
                ))}
            </ul>
        ),
    },
];
