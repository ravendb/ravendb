import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";

export default function useEditAiAgentSubAgentsSection() {
    const { control, trigger } = useFormContext<EditAiAgentFormData>();

    const subAgents = useWatch({
        control,
        name: "subAgents",
    });

    const fieldArray = useFieldArray({
        name: "subAgents",
        control,
    });

    const handleAdd = () => {
        fieldArray.append({
            identifier: "",
            description: "",
            isEditing: true,
        });
    };

    const handleSave = async (index: number) => {
        const isValid = await trigger([`subAgents.${index}`]);
        if (!isValid) {
            return;
        }

        fieldArray.update(index, {
            ...subAgents[index],
            isEditing: false,
        });
    };

    const handleEdit = (index: number) => {
        fieldArray.update(index, {
            ...subAgents[index],
            isEditing: true,
        });
    };

    const handleRemove = (index: number) => {
        fieldArray.remove(index);
    };

    return {
        fieldArray,
        handleAdd,
        handleSave,
        handleEdit,
        handleRemove,
    };
}
