import { FlexGrow } from "components/common/FlexGrow";
import { FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
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

type FormData = ConnectionFormData<AiConnection>;

export default function OllamaSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("ollamaSettings");
        if (!isValid) {
            return;
        }

        return tasksService.testAiConnectionString(databaseName, "Ollama", formValues.modelType, {
            Model: formValues.ollamaSettings.model,
            Uri: formValues.ollamaSettings.uri,
        });
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
                },
            };

            try {
                const result = await tasksService.getAiModels(dto);
                return [...result].sort().map((x) => ({ label: x, value: x }) satisfies SelectOption);
            } catch {
                return [];
            }
        },
        [formValues.ollamaSettings.uri],
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
