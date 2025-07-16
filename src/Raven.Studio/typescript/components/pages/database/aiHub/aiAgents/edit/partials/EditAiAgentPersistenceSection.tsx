import { FormDurationPicker, FormGroup, FormLabel, FormSelectAutocomplete, FormSwitch } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { editAiAgentSelectors } from "../store/editAiAgentSlice";
import OptionalLabel from "components/common/OptionalLabel";

export default function EditAiAgentPersistenceSection() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const isDocumentExpirationEnabled = useAppSelector(editAiAgentSelectors.isDocumentExpirationEnabled);

    const collectionOptions: SelectOption[] = useAppSelector(collectionsTrackerSelectors.collectionNames).map((x) => ({
        value: x,
        label: x,
    }));

    return (
        <>
            <h3 className="m-0 mt-3">Set chat persistence</h3>
            <div className="mb-1">TODO</div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>Conversation ID prefix</FormLabel>
                    <FormSelectAutocomplete
                        control={control}
                        name="persistenceConversationIdPrefix"
                        options={collectionOptions}
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
                        <FormLabel>
                            Expire in <OptionalLabel />
                        </FormLabel>
                        <FormDurationPicker control={control} name="persistenceExpiresInSeconds" showDays isFlexGrow />
                    </FormGroup>
                )}
            </div>
        </>
    );
}
