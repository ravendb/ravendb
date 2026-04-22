import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { FlexGrow } from "components/common/FlexGrow";
import { FormSelect, FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useAppSelector } from "components/store";
import { useAsyncCallback } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import RichAlert from "components/common/RichAlert";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";
import assertUnreachable from "components/utils/assertUnreachable";
import PromptCacheField from "./PromptCacheField";

type FormData = ConnectionFormData<AiConnection>;

export default function GoogleSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("googleSettings");
        if (!isValid) {
            return;
        }

        return tasksService.testAiConnectionString(databaseName, "Google", formValues.modelType, {
            AiVersion: formValues.googleSettings.aiVersion,
            ApiKey: formValues.googleSettings.apiKey,
            EnablePromptCache: formValues.googleSettings.enablePromptCache,
            Model: formValues.googleSettings.model,
            Endpoint: formValues.googleSettings.endpoint,
        });
    });

    return (
        <>
            <RichAlert variant="info">
                This configuration supports Google AI only. Not compatible with Vertex AI.
            </RichAlert>
            <div className="mb-2">
                <FormLabel className="col-form-label">
                    AI Version <OptionalLabel />
                    <PopoverWithHoverWrapper message="The Google AI version to use.">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelect
                    control={control}
                    name="googleSettings.aiVersion"
                    options={
                        [
                            { label: "V1", value: "V1" },
                            { label: "V1_Beta", value: "V1_Beta" },
                        ] satisfies SelectOption<FormData["googleSettings"]["aiVersion"]>[]
                    }
                    isDisabled={isUsedByAnyTask}
                    isClearable
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    API Key
                    <PopoverWithHoverWrapper message="The API key used to authenticate requests to Google's AI services.">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>

                <FormInput control={control} name="googleSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Endpoint <OptionalLabel />
                    <PopoverWithHoverWrapper message="The endpoint for generating responses using Google's AI services.">
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="googleSettings.endpoint"
                    placeholder="Select an endpoint (or enter new one)"
                    options={endpointOptions}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Model
                    <PopoverWithHoverWrapper message="The Google AI text embedding model to use.">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="googleSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder="Select a model (or enter new one)"
                    options={getModelOptions(formValues.modelType)}
                />
            </div>
            <PromptCacheField baseName="googleSettings" serverDefaultValue={false} />
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
                        name="googleSettings.dimensions"
                        type="number"
                        disabled={isUsedByAnyTask}
                    />
                </div>
            )}
            {formValues.modelType === "TextEmbeddings" && <EmbeddingsMaxConcurrentBatches baseName="googleSettings" />}
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

// For now we use hardcoded values. Only these models have been tested
function getModelOptions(modelType: FormData["modelType"]): SelectOption[] {
    switch (modelType) {
        case "Chat":
            return chatModelOptions;
        case "TextEmbeddings":
            return embeddingsModelOptions;
        default:
            assertUnreachable(modelType);
    }
}

const embeddingsModelOptions: SelectOption[] = [
    ...[
        "text-embedding-004",
        "text-embedding-005",
        "textembedding-gecko@001",
        "textembedding-gecko@002",
        "textembedding-gecko@003",
        "textembedding-gecko-multilingual@001",
        "text-multilingual-embedding-002",
    ].map((x) => ({ label: x, value: x })),
    { value: "text-embedding-large-exp-03-07", label: "text-embedding-large-exp-03-07 (experimental)" },
];

const chatModelOptions: SelectOption[] = ["gemini-3-pro-preview", "gemini-3-flash-preview"].map((x) => ({
    label: x,
    value: x,
}));

const endpointOptions: SelectOption[] = ["https://generativelanguage.googleapis.com"].map((x) => ({
    label: x,
    value: x,
}));
