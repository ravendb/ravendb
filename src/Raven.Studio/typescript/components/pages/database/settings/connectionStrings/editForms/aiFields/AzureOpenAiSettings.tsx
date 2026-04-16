import { FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext, useWatch } from "react-hook-form";
import { useAsyncCallback } from "react-async-hook";
import { FlexGrow } from "components/common/FlexGrow";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { SelectOption } from "components/common/select/Select";
import TemperatureField from "./TemperatureField";
import PromptCacheField from "./PromptCacheField";

export default function AzureOpenAiSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<ConnectionFormData<AiConnection>>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("azureOpenAiSettings");
        if (!isValid) {
            return;
        }

        return tasksService.testAiConnectionString(databaseName, "AzureOpenAi", formValues.modelType, {
            ApiKey: formValues.azureOpenAiSettings.apiKey,
            Endpoint: formValues.azureOpenAiSettings.endpoint,
            EnablePromptCache: formValues.azureOpenAiSettings.enablePromptCache,
            Model: formValues.azureOpenAiSettings.model,
            DeploymentName: formValues.azureOpenAiSettings.deploymentName,
            Dimensions: formValues.azureOpenAiSettings.dimensions,
            Temperature: formValues.azureOpenAiSettings.isSetTemperature
                ? formValues.azureOpenAiSettings.temperature
                : null,
        });
    });

    const asyncGetModelOptions = useAsyncDebounce(
        async () => {
            const apiKey = formValues.azureOpenAiSettings.apiKey?.trim() ?? "";
            const endpoint = formValues.azureOpenAiSettings.endpoint?.trim() ?? "";

            if (!apiKey || !endpoint) {
                return [];
            }

            const dto: AiModelsRequestDto = {
                ConnectorType: "AzureOpenAi",
                AzureOpenAiSettings: {
                    ApiKey: apiKey,
                    Endpoint: endpoint,
                },
            };

            try {
                const result = await tasksService.getAiModels(dto);
                return [...result].sort().map((x) => ({ label: x, value: x }) satisfies SelectOption);
            } catch {
                return [];
            }
        },
        [formValues.azureOpenAiSettings.apiKey, formValues.azureOpenAiSettings.endpoint],
        300
    );

    return (
        <>
            <div className="mb-2">
                <FormLabel>
                    API Key
                    <PopoverWithHoverWrapper message="The API key used to authenticate requests to the Azure OpenAI service.">
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>

                <FormInput control={control} name="azureOpenAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Endpoint
                    <PopoverWithHoverWrapper message="The Azure OpenAI endpoint for generating responses.">
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>

                <FormInput control={control} name="azureOpenAiSettings.endpoint" type="text" />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Model
                    <PopoverWithHoverWrapper message="The Azure OpenAI model to use.">
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="azureOpenAiSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder="Select a model or enter a new one (provide API key and Endpoint to see available models)"
                    options={asyncGetModelOptions.result ?? []}
                    isLoading={asyncGetModelOptions.loading}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Deployment Name
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                The name of the deployed Azure OpenAI model to use.
                                <br />
                                <a href="https://learn.microsoft.com/azure/cognitive-services/openai/how-to/create-resource">
                                    Learn more
                                </a>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" id="deploymentName" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="azureOpenAiSettings.deploymentName" type="text" />
            </div>
            {formValues.modelType === "TextEmbeddings" && (
                <div className="mb-2">
                    <FormLabel>
                        Dimensions <OptionalLabel />
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    The number of dimensions for the output embeddings.
                                    <br />
                                    Supported only in &quot;text-embedding-3&quot; and later models.
                                </>
                            }
                        >
                            <Icon icon="info" color="info" id="dimensions" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput
                        control={control}
                        name="azureOpenAiSettings.dimensions"
                        type="number"
                        disabled={isUsedByAnyTask}
                    />
                </div>
            )}
            <PromptCacheField baseName="azureOpenAiSettings" serverDefaultValue={true} />
            <TemperatureField baseName="azureOpenAiSettings" />
            {formValues.modelType === "TextEmbeddings" && (
                <EmbeddingsMaxConcurrentBatches baseName="azureOpenAiSettings" />
            )}
            <div className="d-flex mb-2">
                <FlexGrow />
                <ButtonWithSpinner
                    variant="secondary"
                    icon="rocket"
                    onClick={asyncTest.execute}
                    isSpinning={asyncTest.loading}
                >
                    Test connection
                </ButtonWithSpinner>
            </div>
            {asyncTest.result && <ConnectionTestResult testResult={asyncTest.result} />}
        </>
    );
}
