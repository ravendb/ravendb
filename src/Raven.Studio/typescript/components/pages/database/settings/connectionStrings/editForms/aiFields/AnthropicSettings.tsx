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
import { connectionStringSelectors } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { useFormContext, useWatch } from "react-hook-form";
import { SelectOption } from "components/common/select/Select";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import PromptCacheField from "./PromptCacheField";

type FormData = ConnectionFormData<AiConnection>;

export default function AnthropicSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("anthropicSettings");
        if (!isValid) {
            return;
        }

        const settings = {
            ApiKey: formValues.anthropicSettings.apiKey,
            Model: formValues.anthropicSettings.model,
            Endpoint: formValues.anthropicSettings.endpoint,
            MaxOutputTokens: formValues.anthropicSettings.maxOutputTokens,
            ReasoningEffort: formValues.anthropicSettings.reasoningEffort,
            EnablePromptCache: formValues.anthropicSettings.enablePromptCache,
            ApiVersion: formValues.anthropicSettings.apiVersion,
        };
        return isServerWide
            ? tasksService.testServerWideAiConnectionString("Anthropic", formValues.modelType, settings)
            : tasksService.testAiConnectionString(databaseName, "Anthropic", formValues.modelType, settings);
    });

    const asyncGetModelOptions = useAsyncDebounce(
        async () => {
            const apiKey = formValues.anthropicSettings.apiKey?.trim() ?? "";
            const endpoint = formValues.anthropicSettings.endpoint?.trim() ?? "";

            if (!apiKey) {
                return [];
            }

            const dto: AiModelsRequestDto = {
                ConnectorType: "Anthropic",
                AnthropicSettings: {
                    ApiKey: apiKey,
                    Endpoint: endpoint,
                    ApiVersion: formValues.anthropicSettings.apiVersion,
                },
            };

            try {
                const result = await tasksService.getAiModels(dto);
                return result.map((x) => ({ label: x, value: x }) satisfies SelectOption);
            } catch {
                return [];
            }
        },
        [
            formValues.anthropicSettings.apiKey,
            formValues.anthropicSettings.endpoint,
            formValues.anthropicSettings.apiVersion,
        ],
        300
    );

    return (
        <>
            <div className="mb-2">
                <FormLabel>
                    API Key
                    <PopoverWithHoverWrapper message="The API key used to authenticate requests to Anthropic's Claude API.">
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="anthropicSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Endpoint <OptionalLabel />
                    <PopoverWithHoverWrapper message="The Claude API endpoint. If not set, https://api.anthropic.com/v1/ is used.">
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="anthropicSettings.endpoint"
                    placeholder="Select an endpoint (or enter new one)"
                    options={endpointOptions}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Model
                    <PopoverWithHoverWrapper message="The Claude model to use.">
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="anthropicSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder="Select a model or enter a new one (provide API key to see available models)"
                    options={asyncGetModelOptions.result ?? []}
                    isLoading={asyncGetModelOptions.loading}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Max output tokens <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                The maximum number of tokens Claude can generate in a single response, sent as{" "}
                                <code>max_tokens</code>. Thinking tokens count toward this limit. If not set,{" "}
                                <code>{defaultMaxOutputTokens}</code> is used.
                            </>
                        }
                    >
                        <Icon icon="info" color="info" id="maxOutputTokens" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    control={control}
                    name="anthropicSettings.maxOutputTokens"
                    type="number"
                    placeholder={`Default: ${defaultMaxOutputTokens}`}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Reasoning effort <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <p>
                                    Turns on adaptive thinking and sets how much reasoning Claude does before answering.
                                    Lower values reduce latency and cost.
                                </p>
                                <p>
                                    Typical values are <code>low</code>, <code>medium</code>, <code>high</code>,{" "}
                                    <code>xhigh</code> and <code>max</code>. Effort requires Claude 4.6 or later, and
                                    not every model accepts every level.
                                </p>
                                <p className="mb-0">
                                    The value is sent in lowercase and is not validated by RavenDB. If not set, no
                                    reasoning configuration is sent and the model applies its own default.
                                </p>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" id="reasoningEffort" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="anthropicSettings.reasoningEffort"
                    placeholder="Select a reasoning effort or enter a new one (leave empty for model default)"
                    options={reasoningEffortOptions}
                    isClearable
                />
            </div>
            <PromptCacheField
                baseName="anthropicSettings"
                serverDefaultValue={true}
                label="Prompt caching"
                description={
                    <>
                        Caches the prompt sent to Anthropic, for AI agents and GenAI tasks. Anthropic bills a cache
                        write above the normal input price and a cache read well below it, so caching pays off only when
                        the same prompt is sent again within the cache lifetime: the turns of an AI agent conversation,
                        or a GenAI task whose query tools the model calls. A GenAI task without query tools sends each
                        document once, so caching would only add the write cost.
                        <br />
                        <br />
                        <ul className="mb-0">
                            <li>
                                <strong>Default:</strong> Cache AI agents and GenAI tasks with query tools.
                            </li>
                            <li className="mt-1">
                                <strong>True:</strong> Cache prompts.
                            </li>
                            <li className="mt-1">
                                <strong>False:</strong> Never cache.
                            </li>
                        </ul>
                    </>
                }
            />
            <div className="mb-2">
                <FormLabel>
                    API version <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                The Anthropic API version, sent as the <code>anthropic-version</code> header. If not
                                set, <code>{defaultApiVersion}</code> is used.
                            </>
                        }
                    >
                        <Icon icon="info" color="info" id="apiVersion" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    control={control}
                    name="anthropicSettings.apiVersion"
                    type="text"
                    placeholder={`Default: ${defaultApiVersion}`}
                />
            </div>
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

const defaultMaxOutputTokens = 16000;

const defaultApiVersion = "2023-06-01";

const endpointOptions: SelectOption[] = ["https://api.anthropic.com/v1/"].map((x) => ({ label: x, value: x }));

const reasoningEffortOptions: SelectOption[] = ["low", "medium", "high", "xhigh", "max"].map((x) => ({
    label: x,
    value: x,
}));
