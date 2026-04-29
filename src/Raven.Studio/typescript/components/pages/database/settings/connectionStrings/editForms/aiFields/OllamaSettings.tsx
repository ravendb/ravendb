import { FlexGrow } from "components/common/FlexGrow";
import { FormInput, FormLabel, FormSelect, FormSelectAutocomplete } from "components/common/Form";
import { Icon } from "components/common/Icon";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext, useWatch } from "react-hook-form";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";
import { SelectOption } from "components/common/select/Select";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import TemperatureField from "./TemperatureField";

type FormData = ConnectionFormData<AiConnection>;

export default function OllamaSettings({
    isUsedByAnyTask,
    isServerwide,
}: {
    isUsedByAnyTask: boolean;
    isServerwide?: boolean;
}) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("ollamaSettings");
        if (!isValid) {
            return;
        }

        const settings = {
            Model: formValues.ollamaSettings.model,
            Uri: formValues.ollamaSettings.uri,
            Think: formValues.ollamaSettings.think,
            Temperature: formValues.ollamaSettings.isSetTemperature ? formValues.ollamaSettings.temperature : null,
        };
        return isServerwide
            ? tasksService.testServerWideAiConnectionString("Ollama", formValues.modelType, settings)
            : tasksService.testAiConnectionString(databaseName, "Ollama", formValues.modelType, settings);
    });

    const asyncGetModelOptions = useAsyncDebounce(
        async () => {
            const uri = formValues.ollamaSettings.uri?.trim() ?? "";

            if (!uri) {
                return [];
            }

            const dto: AiModelsRequestDto = {
                ConnectorType: "Ollama",
                OllamaSettings: {
                    Uri: uri,
                    Think: formValues.ollamaSettings.think,
                },
            };

            try {
                const result = await tasksService.getAiModels(dto);
                return [...result].sort().map((x) => ({ label: x, value: x }) satisfies SelectOption);
            } catch {
                return [];
            }
        },
        [formValues.ollamaSettings.uri, formValues.ollamaSettings.think],
        300
    );

    return (
        <>
            <div className="mb-2">
                <FormLabel>
                    URI
                    <PopoverWithHoverWrapper message="The Ollama API URI to use.">
                        <Icon icon="info" color="info" id="uri" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="ollamaSettings.uri" type="text" />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Model
                    <PopoverWithHoverWrapper message="The Ollama model to use.">
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="ollamaSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder="Select a model or enter a new one (provide URI to see available models)"
                    options={asyncGetModelOptions.result ?? []}
                    isLoading={asyncGetModelOptions.loading}
                />
            </div>

            {formValues.modelType === "Chat" && (
                <div className="mb-2">
                    <FormLabel>
                        Thinking mode
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    Controls whether the model outputs its internal reasoning steps before returning the
                                    final answer.
                                    <ul className="mb-1">
                                        <li className="mt-1">
                                            <strong>When &#34;Enabled&#34;:</strong>
                                            <br />
                                            The model outputs a series of intermediate reasoning steps before the final
                                            answer. This may improve output quality for complex tasks, but increases
                                            response time and token usage.
                                        </li>
                                        <li className="mt-1">
                                            <strong>When &#34;Disabled&#34;:</strong>
                                            <br />
                                            The model returns only the final answer, without exposing intermediate
                                            steps. This is typically faster and more cost-effective (uses fewer tokens),
                                            but may reduce quality on complex reasoning.
                                        </li>
                                        <li className="mt-1">
                                            <strong> When setting to “Default”:</strong>
                                            <br />
                                            The model&apos;s built-in default will be used. This value may vary
                                            depending on the selected model.
                                        </li>
                                    </ul>
                                    <p>
                                        Set this parameter based on the trade-off between task complexity and speed/cost
                                        requirements.
                                    </p>
                                </>
                            }
                        >
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormSelect control={control} name="ollamaSettings.think" options={thinkOptions} />
                </div>
            )}
            <TemperatureField baseName="ollamaSettings" />
            {formValues.modelType === "TextEmbeddings" && <EmbeddingsMaxConcurrentBatches baseName="ollamaSettings" />}
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

const thinkOptions: SelectOption<boolean>[] = [
    { label: "Default", value: null },
    { label: "Enabled", value: true },
    { label: "Disabled", value: false },
];
