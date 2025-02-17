import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext, useWatch } from "react-hook-form";
import { useAsyncCallback } from "react-async-hook";
import { Label, UncontrolledPopover, PopoverBody } from "reactstrap";
import { FlexGrow } from "components/common/FlexGrow";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export default function AzureOpenAiSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<ConnectionFormData<AiConnection>>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("azureOpenAiSettings");
        if (!isValid) {
            return;
        }

        return tasksService.testAiConnectionString(databaseName, "AzureOpenAi", {
            ApiKey: formValues.azureOpenAiSettings.apiKey,
            Endpoint: formValues.azureOpenAiSettings.endpoint,
            Model: formValues.azureOpenAiSettings.model,
            DeploymentName: formValues.azureOpenAiSettings.deploymentName,
            Dimensions: formValues.azureOpenAiSettings.dimensions,
        });
    });

    return (
        <>
            <div className="mb-2">
                <Label>
                    API Key
                    <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    <UncontrolledPopover target="apiKey" trigger="hover" className="bs5">
                        <PopoverBody>The API key to used to authenticate with the service.</PopoverBody>
                    </UncontrolledPopover>
                </Label>

                <FormInput control={control} name="azureOpenAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>
                    Endpoint <OptionalLabel />
                    <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    <UncontrolledPopover target="endpoint" trigger="hover" className="bs5">
                        <PopoverBody>The service endpoint that the client will send requests to.</PopoverBody>
                    </UncontrolledPopover>
                </Label>

                <FormInput control={control} name="azureOpenAiSettings.endpoint" type="text" />
            </div>
            <div className="mb-2">
                <Label>
                    Model
                    <Icon icon="info" color="info" id="model" margin="ms-1" />
                    <UncontrolledPopover target="model" trigger="hover" className="bs5">
                        <PopoverBody>The model that should be used.</PopoverBody>
                    </UncontrolledPopover>
                </Label>

                <FormInput control={control} name="azureOpenAiSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>
                    Deployment Name
                    <Icon icon="info" color="info" id="deploymentName" margin="ms-1" />
                    <UncontrolledPopover target="deploymentName" trigger="hover" className="bs5">
                        <PopoverBody>
                            AzureOpenAI deployment name.
                            <br />
                            <a href="https://learn.microsoft.com/azure/cognitive-services/openai/how-to/create-resource">
                                Learn more
                            </a>
                        </PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="azureOpenAiSettings.deploymentName" type="text" />
            </div>
            <div className="mb-2">
                <Label>
                    Dimensions <OptionalLabel />
                    <Icon icon="info" color="info" id="dimensions" margin="ms-1" />
                    <UncontrolledPopover target="dimensions" trigger="hover" className="bs5">
                        <PopoverBody>
                            The number of dimensions the resulting output embeddings should have.
                            <br />
                            <br />
                            Only supported in &quot;text-embedding-3&quot; and later models.
                        </PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput
                    control={control}
                    name="azureOpenAiSettings.dimensions"
                    type="number"
                    disabled={isUsedByAnyTask}
                />
            </div>
            <div className="d-flex mb-2">
                <FlexGrow />
                <ButtonWithSpinner
                    color="secondary"
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
