import { FormSelect, FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import { SelectOption } from "components/common/select/Select";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext } from "react-hook-form";
import { Label, UncontrolledPopover, PopoverBody } from "reactstrap";

type FormData = ConnectionFormData<AiConnection>;

export default function GoogleSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control } = useFormContext<FormData>();

    return (
        <>
            <div className="mb-2">
                <Label className="col-form-label">
                    AI Version <OptionalLabel />
                    <Icon icon="info" color="info" id="aiVersion" margin="ms-1" />
                    <UncontrolledPopover target="aiVersion" trigger="hover" className="bs5">
                        <PopoverBody>The version of the Google AI.</PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormSelect
                    control={control}
                    name="googleSettings.aiVersion"
                    options={
                        [
                            { label: "V1", value: "V1" },
                            { label: "V1_Beta", value: "V1_Beta" },
                        ] satisfies SelectOption<FormData["googleSettings"]["aiVersion"]>[]
                    }
                    isDisabled={isUsedByAnyTask}
                    isClearable
                />
            </div>
            <div className="mb-2">
                <Label>
                    API Key
                    <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    <UncontrolledPopover target="apiKey" trigger="hover" className="bs5">
                        <PopoverBody>The API key to used to authenticate with the service.</PopoverBody>
                    </UncontrolledPopover>
                </Label>

                <FormInput control={control} name="googleSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>
                    Model
                    <Icon icon="info" color="info" id="model" margin="ms-1" />
                    <UncontrolledPopover target="model" trigger="hover" className="bs5">
                        <PopoverBody>The model that should be used.</PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="googleSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
        </>
    );
}
