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

    const handleAddAction = () => {
        actionsFieldArray.append({
            name: "",
            description: "",
            parametersSchema: "",
            isEditing: true,
        });
    };

    const handleSaveAction = (index: number) => {
        actionsFieldArray.update(index, {
            ...formValues.actions[index],
            isEditing: false,
        });
    };

    const handleEditAction = (index: number) => {
        actionsFieldArray.update(index, {
            ...formValues.actions[index],
            isEditing: true,
        });
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
        handleAddAction,
        handleRemoveAction,
        handleSaveAction,
        handleEditAction,
    };
}
