import { FormDurationPicker, FormGroup, FormInput, FormLabel, FormSwitch } from "components/common/Form";
import { useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { ChatAiAgentFormData } from "../utils/chatAiAgentValidation";
import { chatAiAgentSelectors } from "../store/chatAiAgentSlice";

export default function ChatAiAgentPersistenceSection() {
    const { control } = useFormContext<ChatAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const isDocumentExpirationEnabled = useAppSelector(chatAiAgentSelectors.isDocumentExpirationEnabled);

    return (
        <div>
            <h3>
                <Icon icon="press-releases" color="primary" /> Define conversation ID or prefix and expiration
                <PopoverWithHoverWrapper
                    message={
                        <>
                            Chat conversations are stored as documents in the <code>@conversations</code> collection.
                            <br />
                            <br />
                            Configure the document ID prefix and whether these documents should expire after a set
                            period of time.
                        </>
                    }
                >
                    <Icon icon="info-new" />
                </PopoverWithHoverWrapper>
            </h3>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>
                        Conversation ID or prefix
                        <PopoverWithHoverWrapper message="Prefix to use in the document ID of each saved chat created with this agent.">
                            <Icon icon="info-new" />
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
                    <FormGroup marginClass="mb-1">
                        <FormSwitch control={control} name="isEnableDocumentExpiration">
                            Enable document expiration
                        </FormSwitch>
                    </FormGroup>
                )}
                {(formValues.isEnableDocumentExpiration || isDocumentExpirationEnabled.data) && (
                    <FormGroup marginClass="mb-0">
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
                                <Icon icon="info-new" />
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
        </div>
    );
}
