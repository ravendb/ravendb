import { FormDurationPicker, FormGroup, FormInput, FormLabel, FormSwitch } from "components/common/Form";
import { useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { editAiAgentSelectors } from "../store/editAiAgentSlice";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";

export default function EditAiAgentPersistenceSection() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const isDocumentExpirationEnabled = useAppSelector(editAiAgentSelectors.isDocumentExpirationEnabled);

    return (
        <>
            <h3 className="m-0 mt-3">Set chat persistence</h3>
            <div className="mb-1">
                Chat conversations are stored as documents in the <code>@conversations</code> collection.
                <br /> Configure the document ID prefix and whether these documents should expire after a set period of
                time.
            </div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>
                        Conversation ID prefix
                        <PopoverWithHoverWrapper message="Prefix to use in the document ID of each saved chat created with this agent.">
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput
                        control={control}
                        name="persistenceConversationIdPrefix"
                        type="text"
                        placeholder="e.g. Chats/"
                    />
                </FormGroup>
                {isDocumentExpirationEnabled.status === "success" && !isDocumentExpirationEnabled.data && (
                    <FormGroup>
                        <FormSwitch control={control} name="isEnableDocumentExpiration">
                            Enable document expiration
                        </FormSwitch>
                    </FormGroup>
                )}
                {(formValues.isEnableDocumentExpiration || isDocumentExpirationEnabled.data) && (
                    <FormGroup>
                        <FormSwitch name="isDocumentExpireInCustomizeEnabled" control={control}>
                            Set expiration
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        Toggle on to automatically delete conversation documents after a specified
                                        period.
                                        <br />
                                        If disabled, these documents will be retained indefinitely.
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </FormSwitch>
                        {formValues.isDocumentExpireInCustomizeEnabled && (
                            <FormDurationPicker
                                control={control}
                                name="persistenceExpiresInSeconds"
                                showDays
                                isFlexGrow
                            />
                        )}
                    </FormGroup>
                )}
            </div>
        </>
    );
}
