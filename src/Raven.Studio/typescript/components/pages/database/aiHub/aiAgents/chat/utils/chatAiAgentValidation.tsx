import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import { aiAgentParametersUtils } from "../../utils/aiAgentParametersUtils";

type AiAgentParameterValueType = Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType;

const schema = yup.object({
    prompts: yup.array().of(
        yup.object({
            text: yup.string().nullable().required(),
        })
    ),
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
