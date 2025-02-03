import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext } from "react-hook-form";
import { Label, UncontrolledPopover, PopoverBody } from "reactstrap";

type FormData = ConnectionFormData<AiConnection>;

export default function OpenAiSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control } = useFormContext<FormData>();

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
                <FormInput control={control} name="openAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>
                    Endpoint <OptionalLabel />
                    <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    <UncontrolledPopover target="endpoint" trigger="hover" className="bs5">
                        <PopoverBody>The service endpoint that the client will send requests to.</PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="openAiSettings.endpoint" type="text" />
            </div>
            <div className="mb-2">
                <Label>
                    Model
                    <Icon icon="info" color="info" id="model" margin="ms-1" />
                    <UncontrolledPopover target="model" trigger="hover" className="bs5">
                        <PopoverBody>The model that should be used.</PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="openAiSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>
                    Organization ID <OptionalLabel />
                    <Icon icon="info" color="info" id="organizationId" margin="ms-1" />
                    <UncontrolledPopover target="organizationId" trigger="hover" className="bs5">
                        <PopoverBody>
                            The value to use for the <code>OpenAI-Organization</code> request header. Users who belong
                            to multiple organizations can set this value to specify which organization is used for an
                            API request. Usage from these API requests will count against the specified
                            organization&apos;s quota. If not set, the header will be omitted, and the default
                            organization will be billed. You can change your default organization in your user settings.
                            <br />
                            <a href="https://platform.openai.com/docs/guides/production-best-practices/setting-up-your-organization">
                                Learn more
                            </a>
                        </PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="openAiSettings.organizationId" type="text" />
            </div>
            <div className="mb-2">
                <Label>
                    Project ID <OptionalLabel />
                    <Icon icon="info" color="info" id="projectId" margin="ms-1" />
                    <UncontrolledPopover target="projectId" trigger="hover" className="bs5">
                        <PopoverBody>
                            The value to use for the <code>OpenAI-Project</code> request header. Users who are accessing
                            their projects through their legacy user API key can set this value to specify which project
                            is used for an API request. Usage from these API requests will count as usage for the
                            specified project. If not set, the header will be omitted, and the default project will be
                            accessed.
                        </PopoverBody>
                    </UncontrolledPopover>
                </Label>
                <FormInput control={control} name="openAiSettings.projectId" type="text" />
            </div>
        </>
    );
}
