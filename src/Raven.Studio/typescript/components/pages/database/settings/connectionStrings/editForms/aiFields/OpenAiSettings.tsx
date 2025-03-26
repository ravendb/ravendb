import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { FlexGrow } from "components/common/FlexGrow";
import { FormInput, FormSelectCreatable } from "components/common/Form";
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
import { Label } from "reactstrap";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";
import { SelectOption } from "components/common/select/Select";
import { openAiModelOptions } from "../aiConnectionStringUtils";

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

        return tasksService.testAiConnectionString(databaseName, "OpenAi", {
            ApiKey: formValues.openAiSettings.apiKey,
            Endpoint: formValues.openAiSettings.endpoint,
            Model: formValues.openAiSettings.model,
            OrganizationId: formValues.openAiSettings.organizationId,
            ProjectId: formValues.openAiSettings.projectId,
        });
    });

    return (
        <>
            <div className="mb-2">
                <Label>
                    API Key
                    <PopoverWithHoverWrapper message="The API key used to authenticate requests to OpenAI's API.">
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput control={control} name="openAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>
                    Endpoint
                    <PopoverWithHoverWrapper message="The OpenAI endpoint for generating embeddings from text.">
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormSelectCreatable
                    control={control}
                    name="openAiSettings.endpoint"
                    placeholder="Select an endpoint (or enter new one)"
                    options={endpointOptions}
                />
            </div>
            <div className="mb-2">
                <Label>
                    Model
                    <PopoverWithHoverWrapper message="The OpenAI tex embedding model to use.">
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormSelectCreatable
                    control={control}
                    name="openAiSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder="Select a model (or enter new one)"
                    options={openAiModelOptions}
                />
            </div>
            <div className="mb-2">
                <Label>
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
                </Label>
                <FormInput control={control} name="openAiSettings.organizationId" type="text" />
            </div>
            <div className="mb-2">
                <Label>
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
                </Label>
                <FormInput control={control} name="openAiSettings.projectId" type="text" />
            </div>
            <div className="mb-2">
                <Label>
                    Dimensions <OptionalLabel />
                    <PopoverWithHoverWrapper message="The number of dimensions for the output embeddings.">
                        <Icon icon="info" color="info" id="dimensions" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput
                    control={control}
                    name="openAiSettings.dimensions"
                    type="number"
                    disabled={isUsedByAnyTask}
                />
            </div>
            <EmbeddingsMaxConcurrentBatches baseName="openAiSettings" />
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
