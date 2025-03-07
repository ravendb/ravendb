import { FormInput } from "components/common/Form";
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

type FormData = ConnectionFormData<AiConnection>;

export default function MistralaiAiSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("mistralaiAiSettings");
        if (!isValid) {
            return;
        }

        return tasksService.testAiConnectionString(databaseName, "MistralAi", {
            ApiKey: formValues.mistralaiAiSettings.apiKey,
            Endpoint: formValues.mistralaiAiSettings.endpoint,
            Model: formValues.mistralaiAiSettings.model,
        });
    });

    return (
        <>
            <div className="mb-2">
                <Label>
                    API Key
                    <PopoverWithHoverWrapper message="The API key required for accessing the Mistral AI service.">
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput control={control} name="mistralaiAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>
                    Endpoint
                    <PopoverWithHoverWrapper message="The endpoint for the Mistral AI service.">
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput control={control} name="mistralaiAiSettings.endpoint" type="text" />
            </div>
            <div className="mb-2">
                <Label>
                    Model
                    <PopoverWithHoverWrapper message="The model ID for the Mistral AI service.">
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput control={control} name="mistralaiAiSettings.model" type="text" disabled={isUsedByAnyTask} />
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
