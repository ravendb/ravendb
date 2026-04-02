import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import { aiAgentParametersUtils } from "../../utils/aiAgentParametersUtils";

type AiAgentParameterValueType = Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType;

interface ChatAiAgentAttachmentBase {
    name: string;
    contentType: string;
}

interface ChatAiAgentAttachmentLocalFile extends ChatAiAgentAttachmentBase {
    type: "localFile";
    file: File;
}

interface ChatAiAgentAttachmentDocument extends ChatAiAgentAttachmentBase {
    type: "documentAttachment";
    sourceDocumentId: string;
    originalName: string;
}

export type ChatAiAgentAttachment = ChatAiAgentAttachmentLocalFile | ChatAiAgentAttachmentDocument;

const schema = yup.object({
    prompts: yup
        .array()
        .of(
            yup.object({
                text: yup.string().nullable(),
            })
        )
        .test("prompt-or-attachment-required", function (prompts) {
            const attachments = this.parent.attachments;
            const hasAttachments = attachments?.length > 0;
            const hasPrompt = prompts?.some((prompt) => prompt?.text?.trim());

            if (hasPrompt || hasAttachments) {
                return true;
            }

            return this.createError({
                message: prompts?.length > 1 ? "Prompts are required" : "Prompt is required",
            });
        }),
    attachments: yup.array().of(yup.mixed<ChatAiAgentAttachment>()),
    parameters: yup.array().of(
        yup.object({
            name: yup.string().nullable(),
            value: aiAgentParametersUtils
                .createValueSchema(yup.string().nullable())
                .when(["$areParametersRequired", "type"], {
                    is: (areParametersRequired: boolean, type: AiAgentParameterValueType) =>
                        areParametersRequired && type !== "Null",
                    then: (schema) => schema.required(),
                }),
            type: yup.string<AiAgentParameterValueType>(),
            isSendToModel: yup.boolean(),
        })
    ),

    // Persistence
    isEnableDocumentExpiration: yup.boolean(),
    isDocumentExpireInCustomizeEnabled: yup.boolean(),
    persistenceConversationIdPrefix: yup
        .string()
        .nullable()
        .when("$areParametersRequired", {
            is: true,
            then: (schema) => schema.required(),
        }),
    persistenceExpiresInSeconds: yup.number().nullable().positive().integer(),
});

export const chatAiAgentYupResolver = yupResolver(schema);
export type ChatAiAgentFormData = yup.InferType<typeof schema>;
