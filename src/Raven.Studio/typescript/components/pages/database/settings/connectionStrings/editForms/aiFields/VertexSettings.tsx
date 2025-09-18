import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { FlexGrow } from "components/common/FlexGrow";
import { FormInput, FormLabel, FormSelect, FormSelectAutocomplete } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import RichAlert from "components/common/RichAlert";
import { useServices } from "components/hooks/useServices";
import {
    AiConnection,
    ConnectionFormData,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useAppSelector } from "components/store";
import { useAsyncCallback } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";

type FormData = ConnectionFormData<AiConnection>;

export default function VertexSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("vertexSettings");
        if (!isValid) {
            return;
        }

        return tasksService.testAiConnectionString(databaseName, "Vertex", formValues.modelType, {
            AiVersion: formValues.vertexSettings.aiVersion,
            GoogleCredentialsJson: formValues.vertexSettings.googleCredentialsJson,
            Model: formValues.vertexSettings.model,
            Location: formValues.vertexSettings.location,
            ProjectId: formValues.vertexSettings.projectId,
        });
    });

    return (
        <>
            <RichAlert variant="info">
                This configuration supports Vertex AI embeddings only. Not compatible with Google AI.
            </RichAlert>

            <div className="mb-2">
                <FormLabel className="col-form-label">
                    AI Version <OptionalLabel />
                    <PopoverWithHoverWrapper message="The Vertex AI version to use.">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelect
                    control={control}
                    name="vertexSettings.aiVersion"
                    options={aiVersionOptions}
                    isDisabled={isUsedByAnyTask}
                    isClearable
                />
            </div>

            <div className="mb-2">
                <FormLabel>
                    Google Credentials Json
                    <PopoverWithHoverWrapper message="Google credentials used to authenticate requests to Vertex AI services.">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="vertexSettings.googleCredentialsJson" type="textarea" as="textarea" rows={10} />
            </div>

            <div className="mb-2">
                <FormLabel>
                    Model
                    <PopoverWithHoverWrapper message="The Vertex AI text embedding model to use.">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="vertexSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder="Select a model (or enter new one)"
                    options={modelOptions}
                />
            </div>

            <div className="mb-2">
                <FormLabel>
                    Location
                    <PopoverWithHoverWrapper message="The Google Cloud location/region where your Vertex AI resource is deployed (e.g., us-central1).">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="vertexSettings.location" type="text" />
            </div>

            <div className="mb-2">
                <FormLabel>
                    Project ID
                    <PopoverWithHoverWrapper message="The Google Cloud project ID that owns the Vertex AI resource.">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="vertexSettings.projectId" type="text" />
            </div>

            <EmbeddingsMaxConcurrentBatches baseName="vertexSettings" />

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

const modelOptions: SelectOption[] = [
    "gemini-embedding-001",
    "text-embedding-005",
    "text-multilingual-embedding-002",
].map((x) => ({ label: x, value: x }));

const aiVersionOptions: SelectOption[] = [
    { label: "V1", value: "V1" },
    { label: "V1_Beta", value: "V1_Beta" },
] satisfies SelectOption<FormData["vertexSettings"]["aiVersion"]>[];
