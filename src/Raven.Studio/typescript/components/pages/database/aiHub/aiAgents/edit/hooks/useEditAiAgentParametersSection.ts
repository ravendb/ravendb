import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";

export default function useEditAiAgentParametersSection() {
    const { control, trigger } = useFormContext<EditAiAgentFormData>();

    const parameters = useWatch({
        control,
        name: "parameters",
    });

    const fieldArray = useFieldArray({
        name: "parameters",
        control,
    });

    const handleAdd = () => {
        fieldArray.append({
            name: "",
            description: null,
            isSendToModel: true,
            policy: "Default",
            type: "String",
            isEditing: true,
        });
    };

    const handleSave = async (index: number) => {
        const isValid = await trigger([`parameters.${index}`]);
        if (!isValid) {
            return;
        }

        fieldArray.update(index, {
            ...parameters[index],
            isEditing: false,
        });
    };

    const handleEdit = (index: number) => {
        fieldArray.update(index, {
            ...parameters[index],
            isEditing: true,
        });
    };

    const handleRemove = (index: number) => {
        fieldArray.remove(index);
    };

    return {
        parameters,
        fieldArray,
        handleAdd,
        handleSave,
        handleEdit,
        handleRemove,
    };
}
