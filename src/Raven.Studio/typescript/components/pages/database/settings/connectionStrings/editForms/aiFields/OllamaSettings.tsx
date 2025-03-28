import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext } from "react-hook-form";
import { Label, UncontrolledPopover, PopoverBody } from "reactstrap";

type FormData = ConnectionFormData<AiConnection>;

export default function OllamaSettings<T extends FormData>({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control } = useFormContext<T>();

    return (
        <>
            <div className="mb-2">
                <Label>
                    Model
                    <Icon icon="info" color="info" id="model" margin="ms-1" />
                    <UncontrolledPopover target="model" trigger="hover" className="bs5">
                        <PopoverBody>The model that should be used.</PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="ollamaSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>
                    URI
                    <Icon icon="info" color="info" id="uri" margin="ms-1" />
                    <UncontrolledPopover target="uri" trigger="hover" className="bs5">
                        <PopoverBody>The URI of the Ollama API.</PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="ollamaSettings.uri" type="text" />
            </div>
        </>
    );
}
