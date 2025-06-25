import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { FlexGrow } from "components/common/FlexGrow";
import { FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useAppSelector } from "components/store";
import { useAsyncCallback } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";
import { SelectOption } from "components/common/select/Select";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";

type FormData = ConnectionFormData<AiConnection>;

export default function OpenAiSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("openAiSettings");
        if (!isValid) {
            return;
        }

        return tasksService.testAiConnectionString(databaseName, "OpenAi", formValues.modelType, {
            ApiKey: formValues.openAiSettings.apiKey,
            Endpoint: formValues.openAiSettings.endpoint,
            Model: formValues.openAiSettings.model,
            OrganizationId: formValues.openAiSettings.organizationId,
            ProjectId: formValues.openAiSettings.projectId,
        });
    });

    const asyncGetModelOptions = useAsyncDebounce(
        async () => {
            const apiKey = formValues.openAiSettings.apiKey?.trim() ?? "";
            const endpoint = formValues.openAiSettings.endpoint?.trim() ?? "";
            const projectId = formValues.openAiSettings.projectId?.trim() ?? "";
            const organizationId = formValues.openAiSettings.organizationId?.trim() ?? "";

            if (!apiKey) {
                return [];
            }

            const dto: AiModelsRequestDto = {
                ConnectorType: "OpenAi",
                OpenAiSettings: {
                    ApiKey: apiKey,
                    Endpoint: endpoint || "https://api.openai.com/v1/",
                    OrganizationId: organizationId,
                    ProjectId: projectId,
                },
            };

            try {
                const result = await tasksService.getAiModels(dto);
                return [...result].sort().map((x) => ({ label: x, value: x }) satisfies SelectOption);
            } catch {
                return [];
            }
        },
        [
            formValues.openAiSettings.apiKey,
            formValues.openAiSettings.endpoint,
            formValues.openAiSettings.organizationId,
            formValues.openAiSettings.projectId,
        ],
        300
    );

    return (
        <>
            <div className="mb-2">
                <FormLabel>
                    API Key
                    <PopoverWithHoverWrapper message="The API key used to authenticate requests to OpenAI or any OpenAI-compatible provider.">
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="openAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Endpoint
                    <PopoverWithHoverWrapper message="The endpoint for generating responses using OpenAI or any OpenAI-compatible provider.">
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="openAiSettings.endpoint"
                    placeholder="Select an endpoint (or enter new one)"
                    options={endpointOptions}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Model
                    <PopoverWithHoverWrapper message="The model to use with OpenAI or any OpenAI-compatible provider.">
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="openAiSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder="Select a model or enter a new one (provide API key to see available models)"
                    options={asyncGetModelOptions.result ?? []}
                    isLoading={asyncGetModelOptions.loading}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Organization ID <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <p>
                                    The organization ID to use for the <code>OpenAI-Organization</code> request header.
                                </p>
                                <p>
                                    Users belonging to multiple organizations can set this value to specify which
                                    organization is used for an API request. Usage from these API requests will count
                                    against the specified organization&apos;s quota.
                                </p>
                                <p>
                                    If not set, the header will be omitted, and the default organization will be billed.
                                    You can change your default organization in your user settings.
                                    <br />
                                    <a href="https://platform.openai.com/docs/guides/production-best-practices/setting-up-your-organization">
                                        Learn more
                                    </a>
                                </p>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" id="organizationId" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="openAiSettings.organizationId" type="text" />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Project ID <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <p>
                                    The project ID to use for the <code>OpenAI-Project</code> request header.
                                </p>
                                <p>
                                    Users who are accessing their projects through their legacy user API key can set
                                    this value to specify which project is used for an API request. Usage from these API
                                    requests will count as usage for the specified project.
                                </p>
                                <p>If not set, the header will be omitted, and the default project will be accessed.</p>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" id="projectId" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="openAiSettings.projectId" type="text" />
            </div>
            {formValues.modelType === "TextEmbeddings" && (
                <div className="mb-2">
                    <FormLabel>
                        Dimensions <OptionalLabel />
                        <PopoverWithHoverWrapper message="The number of dimensions for the output embeddings.">
                            <Icon icon="info" color="info" id="dimensions" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput
                        control={control}
                        name="openAiSettings.dimensions"
                        type="number"
                        disabled={isUsedByAnyTask}
                    />
                </div>
            )}
            {formValues.modelType === "TextEmbeddings" && <EmbeddingsMaxConcurrentBatches baseName="openAiSettings" />}
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

const endpointOptions: SelectOption[] = ["https://api.openai.com/v1/"].map((x) => ({ label: x, value: x }));
