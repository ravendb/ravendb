import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext } from "react-hook-form";
import { Label, UncontrolledPopover, PopoverBody } from "reactstrap";

type FormData = ConnectionFormData<AiConnection>;

export default function MistralaiAiSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control } = useFormContext<FormData>();

    return (
        <>
            <div className="mb-2">
                <Label>
                    API Key
                    <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    <UncontrolledPopover target="apiKey" trigger="hover" className="bs5">
                        <PopoverBody>The API key required for accessing the Mistral AI service.</PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="mistralaiAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>
                    Endpoint
                    <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    <UncontrolledPopover target="endpoint" trigger="hover" className="bs5">
                        <PopoverBody>The endpoint for the Mistral AI service.</PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="mistralaiAiSettings.endpoint" type="text" />
            </div>
            <div className="mb-2">
                <Label>
                    Model
                    <Icon icon="info" color="info" id="model" margin="ms-1" />
                    <UncontrolledPopover target="model" trigger="hover" className="bs5">
                        <PopoverBody>The model ID for the Mistral AI service.</PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="mistralaiAiSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
        </>
    );
}
