import { FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { Icon } from "components/common/Icon";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext, useWatch } from "react-hook-form";
import { FlexGrow } from "components/common/FlexGrow";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { connectionStringSelectors } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { useAsyncCallback } from "react-async-hook";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import OptionalLabel from "components/common/OptionalLabel";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";

type FormData = ConnectionFormData<AiConnection>;

export default function HuggingFaceSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("huggingFaceSettings");
        if (!isValid) {
            return;
        }

        const settings = {
            ApiKey: formValues.huggingFaceSettings.apiKey,
            Endpoint: formValues.huggingFaceSettings.endpoint,
            Model: formValues.huggingFaceSettings.model,
        };
        return isServerWide
            ? tasksService.testServerWideAiConnectionString("HuggingFace", formValues.modelType, settings)
            : tasksService.testAiConnectionString(databaseName, "HuggingFace", formValues.modelType, settings);
    });

    return (
        <>
            <div className="mb-2">
                <FormLabel>
                    API Key
                    <PopoverWithHoverWrapper message="The API key used to authenticate requests to Hugging Face's text embedding services.">
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="huggingFaceSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Endpoint <OptionalLabel />
                    <PopoverWithHoverWrapper message="The Hugging Face endpoint for generating embeddings from text. If not specified, the default endpoint is used.">
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="huggingFaceSettings.endpoint"
                    placeholder="Select an endpoint (or enter new one)"
                    options={["https://api-inference.huggingface.com/"].map((x) => ({ label: x, value: x }))}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    Model
                    <PopoverWithHoverWrapper message="The Hugging Face text embedding model to use.">
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="huggingFaceSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
            <EmbeddingsMaxConcurrentBatches baseName="huggingFaceSettings" />
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
