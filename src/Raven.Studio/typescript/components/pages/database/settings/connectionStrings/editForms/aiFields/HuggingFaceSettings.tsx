import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
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
import RichAlert from "components/common/RichAlert";

type FormData = ConnectionFormData<AiConnection>;

export default function HuggingFaceSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("huggingFaceSettings");
        if (!isValid) {
            return;
        }

        return tasksService.testAiConnectionString(databaseName, "HuggingFace", {
            ApiKey: formValues.huggingFaceSettings.apiKey,
            Endpoint: formValues.huggingFaceSettings.endpoint,
            Model: formValues.huggingFaceSettings.model,
        });
    });

    return (
        <>
            <RichAlert variant="warning">
                Semantic Kernel&apos;s Hugging Face connector does not support batching as of now. Each request to a
                Hugging Face model endpoint will be processed individually.
            </RichAlert>
            <div className="mb-2">
                <Label>
                    API Key <OptionalLabel />
                    <PopoverWithHoverWrapper message="The API key required for accessing the Hugging Face service.">
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput control={control} name="huggingFaceSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>
                    Endpoint
                    <PopoverWithHoverWrapper message="The endpoint for the text embedding generation service. If not specified, the default endpoint will be used.">
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput control={control} name="huggingFaceSettings.endpoint" type="text" />
            </div>
            <div className="mb-2">
                <Label>
                    Model
                    <PopoverWithHoverWrapper message="The name of the Hugging Face model.">
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput control={control} name="huggingFaceSettings.model" type="text" disabled={isUsedByAnyTask} />
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
