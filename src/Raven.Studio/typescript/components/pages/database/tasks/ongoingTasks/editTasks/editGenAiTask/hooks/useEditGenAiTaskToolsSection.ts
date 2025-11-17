import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";

export default function useEditGenAiTaskToolsSection() {
    const { control } = useFormContext<EditGenAiTaskFormData>();

    const formValues = useWatch({
        control,
    });

    const queriesFieldArray = useFieldArray({
        name: "queries",
        control,
    });

    const handleAddQuery = () => {
        queriesFieldArray.append({
            name: "",
            description: "",
            query: "",
            parametersSchema: "",
            isAllowModelQueries: null,
            isAllowModelQueriesOverride: false,
            isAddToInitialContext: null,
            isAddToInitialContextOverride: false,
            isEditing: true,
        });
    };

    const handleSaveQuery = (index: number) => {
        queriesFieldArray.update(index, {
            ...formValues.queries[index],
            isEditing: false,
        });
    };

    const handleEditQuery = (index: number) => {
        queriesFieldArray.update(index, {
            ...formValues.queries[index],
            isEditing: true,
        });
    };

    const handleRemoveQuery = (index: number) => {
        queriesFieldArray.remove(index);
    };

    return {
        queriesFieldArray,
        handleAddQuery,
        handleRemoveQuery,
        handleSaveQuery,
        handleEditQuery,
    };
}
