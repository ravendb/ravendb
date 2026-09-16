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
import { connectionStringSelectors } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { useFormContext, useWatch } from "react-hook-form";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";
import Button from "react-bootstrap/Button";
import useBoolean from "hooks/useBoolean";
import classNames from "classnames";

type FormData = ConnectionFormData<AiConnection>;

interface VertexSettingsProps {
    isUsedByAnyTask: boolean;
    isForNewConnection: boolean;
}

export default function VertexSettings({ isUsedByAnyTask, isForNewConnection }: VertexSettingsProps) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);
    const { value: isCredentialsJsonVisible, toggle: toggleCredentialsJsonVisible } = useBoolean(isForNewConnection);
    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("vertexSettings");
        if (!isValid) {
            return;
        }

        const settings = {
            AiVersion: formValues.vertexSettings.aiVersion,
            GoogleCredentialsJson: formValues.vertexSettings.googleCredentialsJson,
            Model: formValues.vertexSettings.model,
            Location: formValues.vertexSettings.location,
        };
        return isServerWide
            ? tasksService.testServerWideAiConnectionString("Vertex", formValues.modelType, settings)
            : tasksService.testAiConnectionString(databaseName, "Vertex", formValues.modelType, settings);
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
                <FormInput
                    control={control}
                    name="vertexSettings.googleCredentialsJson"
                    type="textarea"
                    placeholder={googleCredentialsJsonPlaceholder}
                    autoComplete="off"
                    as="textarea"
                    rows={15}
                    className={classNames({ "d-none": !isCredentialsJsonVisible })}
                />
                <Button
                    type="button"
                    variant="secondary"
                    className="w-fit-content mt-2"
                    onClick={toggleCredentialsJsonVisible}
                >
                    <Icon icon={isCredentialsJsonVisible ? "preview-off" : "preview"} />
                    {isCredentialsJsonVisible ? "Hide" : "Show"} credentials
                </Button>
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

const googleCredentialsJsonPlaceholder = `e.g.
{
    "type": "service_account",
    "project_id": "test-raven-237012",
    "private_key_id": "12345678123412341234123456789101",
    "private_key": "-----BEGIN PRIVATE KEY-----\\abCse=-----END PRIVATE KEY-----",
    "client_email": "raven@test-raven-237012-237012.iam.gserviceaccount.com",
    "client_id": "111390682349634407434",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/viewonly%40test-raven-237012.iam.gserviceaccount.com"
}`;

const modelOptions: SelectOption[] = [
    "gemini-embedding-001",
    "text-embedding-005",
    "text-multilingual-embedding-002",
].map((x) => ({ label: x, value: x }));

const aiVersionOptions: SelectOption[] = [
    { label: "V1", value: "V1" },
    { label: "V1_Beta", value: "V1_Beta" },
] satisfies SelectOption<FormData["vertexSettings"]["aiVersion"]>[];
