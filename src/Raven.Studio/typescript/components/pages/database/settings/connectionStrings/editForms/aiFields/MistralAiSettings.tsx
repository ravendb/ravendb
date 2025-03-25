import { FormInput, FormSelectCreatable } from "components/common/Form";
import { Icon } from "components/common/Icon";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext, useWatch } from "react-hook-form";
import { Label } from "reactstrap";
import { FlexGrow } from "components/common/FlexGrow";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAsyncCallback } from "react-async-hook";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";
import { SelectOption } from "components/common/select/Select";

type FormData = ConnectionFormData<AiConnection>;

export default function MistralAiSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("mistralAiSettings");
        if (!isValid) {
            return;
        }

        return tasksService.testAiConnectionString(databaseName, "MistralAi", {
            ApiKey: formValues.mistralAiSettings.apiKey,
            Endpoint: formValues.mistralAiSettings.endpoint,
            Model: formValues.mistralAiSettings.model,
        });
    });

    return (
        <>
            <div className="mb-2">
                <Label>
                    API Key
                    <PopoverWithHoverWrapper message="The API key used to authenticate requests to Mistral AI's API.">
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput control={control} name="mistralAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>
                    Endpoint
                    <PopoverWithHoverWrapper message="The Mistral AI endpoint for generating embeddings from text.">
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormSelectCreatable
                    control={control}
                    name="mistralAiSettings.endpoint"
                    placeholder="Select an endpoint (or enter new one)"
                    options={endpointOptions}
                />
            </div>
            <div className="mb-2">
                <Label>
                    Model
                    <PopoverWithHoverWrapper message="The Mistral AI text embedding model ID to use.">
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormSelectCreatable
                    control={control}
                    name="mistralAiSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder="Select a model (or enter new one)"
                    options={modelOptions}
                />
            </div>
            <EmbeddingsMaxConcurrentBatches baseName="mistralAiSettings" />
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

const endpointOptions: SelectOption[] = ["https://api.mistral.ai/v1/"].map((x) => ({ label: x, value: x }));

const modelOptions: SelectOption[] = ["mistral-embed"].map((x) => ({
    label: x,
    value: x,
}));
