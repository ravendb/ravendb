import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppDispatch, useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import { editGenAiTaskSelectors, editGenAiTaskActions } from "../store/editGenAiTaskSlice";
import { editGenAiTaskUtils } from "../utils/editGenAiTaskUtils";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";

export function useEditGenAiTaskTests() {
    const dispatch = useAppDispatch();
    const { control, trigger, setValue } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const taskId = useAppSelector(editGenAiTaskSelectors.taskId);

    const handleContextTest = async () => {
        const areTestRelatedFieldsValid = await trigger(["collectionName", "script"]);
        if (!areTestRelatedFieldsValid) {
            return;
        }

        const dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript = {
            TestStage: "CreateContextObjects",
            Input: null,
            Document: JSON.parse(formValues.playgroundDocument),
            DocumentId: undefined,
            IsDelete: false,
            Configuration: editGenAiTaskUtils.mapToDto(formValues, taskId),
        };

        const result = await dispatch(editGenAiTaskActions.testContext({ databaseName, dto })).unwrap();

        setValue(
            "playgroundContexts",
            result.Results.map((x, idx) => ({
                idx,
                value: JSON.stringify(x.ContextOutput.Context, null, 4),
                aiHash: x.ContextOutput.AiHash,
                isCached: x.ContextOutput.IsCached,
            }))
        );
    };

    const handleModelInputTest = async () => {
        const areTestRelatedFieldsValid = await trigger(["prompt", "sampleObject", "jsonSchema"]);
        if (!areTestRelatedFieldsValid) {
            return;
        }

        const input: Raven.Server.Documents.ETL.Providers.AI.GenAi.GenAiResultItem[] =
            formValues.playgroundContexts.map((x) => {
                return {
                    ContextOutput: {
                        Context: JSON.parse(x.value),
                        AiHash: formValues.isForceSendingCachedObjects ? null : x.aiHash,
                        IsCached: formValues.isForceSendingCachedObjects ? false : x.isCached,
                    },
                    DebugActions: null,
                    DebugOutput: [],
                    ModelOutput: null,
                };
            });

        const dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript = {
            TestStage: "SendToModel",
            Input: input,
            Document: null,
            DocumentId: undefined,
            IsDelete: false,
            Configuration: editGenAiTaskUtils.mapToDto(formValues, taskId),
        };

        const result = await dispatch(editGenAiTaskActions.testModelInput({ databaseName, dto })).unwrap();

        setValue(
            "playgroundContexts",
            result.Results.map((x, idx) => ({
                idx,
                value: JSON.stringify(x.ContextOutput.Context, null, 4),
                aiHash: x.ContextOutput.AiHash,
                isCached: x.ContextOutput.IsCached,
            }))
        );

        setValue(
            "playgroundModelOutputs",
            result.Results.map((x, idx) => ({
                idx,
                value: JSON.stringify(x.ModelOutput?.Output, null, 4),
            }))
        );
    };

    const handleUpdateScriptTest = async () => {
        const areTestRelatedFieldsValid = await trigger(["prompt", "sampleObject", "jsonSchema"]);
        if (!areTestRelatedFieldsValid) {
            return;
        }

        const input: Raven.Server.Documents.ETL.Providers.AI.GenAi.GenAiResultItem[] =
            formValues.playgroundModelOutputs.map((_, idx) => {
                return {
                    ContextOutput: {
                        Context: JSON.parse(formValues.playgroundContexts[idx].value),
                        AiHash: formValues.isForceSendingCachedObjects
                            ? null
                            : formValues.playgroundContexts[idx].aiHash,
                        IsCached: formValues.isForceSendingCachedObjects
                            ? false
                            : formValues.playgroundContexts[idx].isCached,
                    },
                    DebugActions: null,
                    DebugOutput: [],
                    ModelOutput: {
                        Output: JSON.parse(formValues.playgroundModelOutputs[idx].value),
                        Usage: {
                            CachedTokens: 0,
                            CompletionTokens: 0,
                            PromptTokens: 0,
                            TotalTokens: 0,
                        },
                    },
                };
            });

        const dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript = {
            TestStage: "ApplyUpdateScript",
            Input: input,
            Document: JSON.parse(formValues.playgroundDocument),
            DocumentId: undefined,
            IsDelete: false,
            Configuration: editGenAiTaskUtils.mapToDto(formValues, taskId),
        };

        await dispatch(editGenAiTaskActions.testUpdateScript({ databaseName, dto })).unwrap();
    };

    return {
        handleContextTest,
        handleModelInputTest,
        handleUpdateScriptTest,
    };
}
