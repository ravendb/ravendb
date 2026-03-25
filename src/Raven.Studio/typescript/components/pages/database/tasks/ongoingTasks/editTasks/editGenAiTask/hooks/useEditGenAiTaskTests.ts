import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppDispatch, useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import { editGenAiTaskSelectors, editGenAiTaskActions } from "../store/editGenAiTaskSlice";
import { editGenAiTaskUtils } from "../utils/editGenAiTaskUtils";
import { EditGenAiTaskFormData, GenAiAiAttachment } from "../utils/editGenAiTaskValidation";
import messagePublisher from "common/messagePublisher";

type GenAiResultItem = Raven.Server.Documents.ETL.Providers.AI.GenAi.GenAiResultItem;

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
            DocumentId: getDocumentId(formValues),
            IsDelete: false,
            Configuration: editGenAiTaskUtils.mapToDto(formValues, taskId),
        };

        const result = await dispatch(editGenAiTaskActions.testContext({ databaseName, dto })).unwrap();

        if (result.TransformationErrors?.length > 0) {
            messagePublisher.reportError(
                "Failed to create context objects",
                result.TransformationErrors.map((x) => x.Error).join("\n")
            );
            return;
        }

        setValue(
            "playgroundContexts",
            result.Results.map((x, idx) => ({
                idx,
                value: JSON.stringify(x.ContextOutput.Context, null, 4),
                aiHash: x.ContextOutput.AiHash,
                isCached: x.ContextOutput.IsCached,
                attachments: x.ContextOutput.Attachments,
            }))
        );
    };

    const handleModelInputTest = async () => {
        const areTestRelatedFieldsValid = await trigger(["prompt", "sampleObject", "jsonSchema"]);
        if (!areTestRelatedFieldsValid) {
            return;
        }

        const input: GenAiResultItem[] = formValues.playgroundContexts.map((x): GenAiResultItem => {
            return {
                ContextOutput: {
                    Context: JSON.parse(x.value),
                    AiHash: formValues.isForceSendingCachedObjects ? null : x.aiHash,
                    IsCached: formValues.isForceSendingCachedObjects ? false : x.isCached,
                    Attachments: getAttachments(x.attachments),
                },
                ModelOutput: null,
                DocumentId: getDocumentId(formValues),
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

        const input: GenAiResultItem[] = formValues.playgroundModelOutputs.map((_, idx): GenAiResultItem => {
            return {
                ContextOutput: {
                    Context: JSON.parse(formValues.playgroundContexts[idx].value),
                    AiHash: formValues.isForceSendingCachedObjects ? null : formValues.playgroundContexts[idx].aiHash,
                    IsCached: formValues.isForceSendingCachedObjects
                        ? false
                        : formValues.playgroundContexts[idx].isCached,
                    Attachments: getAttachments(formValues.playgroundContexts[idx].attachments),
                },
                ModelOutput: {
                    Output: JSON.parse(formValues.playgroundModelOutputs[idx].value),
                    Usage: {
                        CachedTokens: 0,
                        CompletionTokens: 0,
                        PromptTokens: 0,
                        ReasoningTokens: 0,
                        TotalTokens: 0,
                    },
                    ConversationDocument: undefined,
                },
                DocumentId: getDocumentId(formValues),
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

    const getDocumentId = (formValues: EditGenAiTaskFormData): string => {
        if (!formValues.documentId || !formValues.playgroundDocument) {
            return undefined;
        }

        return formValues.documentId;
    };

    // For type safety (yup creates optional fields)
    const getAttachments = (
        attachments: GenAiAiAttachment[]
    ): Raven.Server.Documents.ETL.Providers.AI.AiAttachment[] => {
        if (!attachments) {
            return null;
        }

        return attachments.map((x) => ({
            Data: x.Data,
            Name: x.Name,
            RemoteStorageId: x.RemoteStorageId,
            Source: x.Source,
            Type: x.Type,
            DownloadDurationInMs: x.DownloadDurationInMs,
        }));
    };

    return {
        handleContextTest,
        handleModelInputTest,
        handleUpdateScriptTest,
    };
}
