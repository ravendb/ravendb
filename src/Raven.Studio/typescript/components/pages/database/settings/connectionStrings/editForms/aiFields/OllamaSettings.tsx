import { FlexGrow } from "components/common/FlexGrow";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext, useWatch } from "react-hook-form";
import { Label } from "reactstrap";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

type FormData = ConnectionFormData<AiConnection>;

export default function OllamaSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("ollamaSettings");
        if (!isValid) {
            return;
        }

        return tasksService.testAiConnectionString(databaseName, "Ollama", {
            Model: formValues.ollamaSettings.model,
            Uri: formValues.ollamaSettings.uri,
        });
    });

    return (
        <>
            <div className="mb-2">
                <Label>
                    Model
                    <PopoverWithHoverWrapper message="The Ollama text embedding model to use.">
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput control={control} name="ollamaSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>
                    URI
                    <PopoverWithHoverWrapper message="The Ollama API URI to use.">
                        <Icon icon="info" color="info" id="uri" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </Label>
                <FormInput control={control} name="ollamaSettings.uri" type="text" />
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
