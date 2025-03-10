import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext, useWatch } from "react-hook-form";
import { useAsyncCallback } from "react-async-hook";
import { Label } from "reactstrap";
import { FlexGrow } from "components/common/FlexGrow";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

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
                    <PopoverWithHoverWrapper message="The API key to use to authenticate with the Azure OpenAI service.">
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>

                <FormInput control={control} name="azureOpenAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>
                    Endpoint
                    <PopoverWithHoverWrapper message="The Azure OpenAI endpoint for generating embeddings from text.">
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>

                <FormInput control={control} name="azureOpenAiSettings.endpoint" type="text" />
            </div>
            <div className="mb-2">
                <Label>
                    Model
                    <PopoverWithHoverWrapper message="The Azure OpenAI text embedding model to use.">
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>

                <FormInput control={control} name="azureOpenAiSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>
                    Deployment Name
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                The name of the deployed Azure OpenAI model to use.
                                <br />
                                <a href="https://learn.microsoft.com/azure/cognitive-services/openai/how-to/create-resource">
                                    Learn more
                                </a>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" id="deploymentName" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput control={control} name="azureOpenAiSettings.deploymentName" type="text" />
            </div>
            <div className="mb-2">
                <Label>
                    Dimensions <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                The number of dimensions for the output embeddings.
                                <br />
                                Supported only in &quot;text-embedding-3&quot; and later models.
                            </>
                        }
                    >
                        <Icon icon="info" color="info" id="dimensions" margin="ms-1" />
                    </PopoverWithHoverWrapper>
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
