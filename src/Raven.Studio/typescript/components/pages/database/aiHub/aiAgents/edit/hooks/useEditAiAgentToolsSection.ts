import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";

export default function useEditAiAgentToolsSection() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const queriesFieldArray = useFieldArray({
        name: "queries",
        control,
    });

    const actionsFieldArray = useFieldArray({
        name: "actions",
        control,
    });

    const handleAddQuery = () => {
        queriesFieldArray.append({
            name: "",
            description: "",
            query: "",
            parametersSchema: "",
            isSaved: false,
            isEditing: true,
        });
    };

    const handleSaveQuery = (index: number) => {
        queriesFieldArray.update(index, {
            ...formValues.queries[index],
            isSaved: true,
            isEditing: false,
        });
    };

    const handleEditQuery = (index: number) => {
        queriesFieldArray.update(index, {
            ...formValues.queries[index],
            isEditing: true,
            prevValue: formValues.queries[index],
        });
    };

    const handleCancelEditQuery = (index: number) => {
        if (formValues.queries[index].isSaved) {
            queriesFieldArray.update(index, {
                ...formValues.queries[index].prevValue,
                isEditing: false,
            });
        }
    };

    const handleRemoveQuery = (index: number) => {
        queriesFieldArray.remove(index);
    };

    const handleAddAction = () => {
        actionsFieldArray.append({
            name: "",
            description: "",
            parametersSchema: "",
            isSaved: false,
            isEditing: true,
        });
    };

    const handleSaveAction = (index: number) => {
        actionsFieldArray.update(index, {
            ...formValues.actions[index],
            isSaved: true,
            isEditing: false,
        });
    };

    const handleEditAction = (index: number) => {
        actionsFieldArray.update(index, {
            ...formValues.actions[index],
            isEditing: true,
            prevValue: formValues.actions[index],
        });
    };

    const handleCancelEditAction = (index: number) => {
        if (formValues.actions[index].isSaved) {
            actionsFieldArray.update(index, {
                ...formValues.actions[index].prevValue,
                isEditing: false,
            });
        }
    };

    const handleRemoveAction = (index: number) => {
        actionsFieldArray.remove(index);
    };

    return {
        queriesFieldArray,
        actionsFieldArray,
        handleAddQuery,
        handleRemoveQuery,
        handleSaveQuery,
        handleEditQuery,
        handleCancelEditQuery,
        handleAddAction,
        handleRemoveAction,
        handleSaveAction,
        handleEditAction,
        handleCancelEditAction,
    };
}
