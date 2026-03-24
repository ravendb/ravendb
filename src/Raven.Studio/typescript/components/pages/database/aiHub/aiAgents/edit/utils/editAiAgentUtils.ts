import { AiAgentTrimmingMethod, EditAiAgentFormData } from "./editAiAgentValidation";

function mapFromDto(
    dto?: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration,
    isClone?: boolean
): Required<EditAiAgentFormData> {
    if (!dto) {
        return {
            name: "",
            identifier: "",
            state: "Enabled",
            connectionStringName: "",
            systemPrompt: "",
            sampleObject: "",
            outputSchema: "",
            canRegenerateSchema: false,
            parameters: [],
            queries: [],
            actions: [],
            trimming: {
                method: "Tokens",
                isEnableHistory: false,
                isSetHistoryExpiration: false,
                historyExpirationInSeconds: null,
                // messagesLengthBeforeTruncate: null,
                // messagesLengthAfterTruncate: null,
                maxTokensBeforeSummarization: null,
                maxTokensAfterSummarization: null,
            },
            subAgents: [],
        };
    }

    return {
        name: isClone ? "" : dto.Name,
        identifier: isClone ? "" : dto.Identifier,
        state: dto.Disabled ? "Disabled" : "Enabled",
        connectionStringName: dto.ConnectionStringName,
        systemPrompt: dto.SystemPrompt,
        sampleObject: dto.SampleObject,
        outputSchema: dto.OutputSchema,
        canRegenerateSchema: false,
        parameters:
            dto.Parameters?.map((x) => ({
                name: x.Name,
                description: x.Description,
                isSendToModel: x.SendToModel ?? true,
                policy: x.Policy ?? "Default",
                type: x.Type ?? "Default",
                isEditing: false,
            })) ?? [],
        queries:
            dto.Queries?.map((x) => ({
                name: x.Name,
                description: x.Description,
                isAllowModelQueries: x.Options?.AllowModelQueries ?? null,
                isAllowModelQueriesOverride: x.Options?.AllowModelQueries != null,
                isAddToInitialContext: x.Options?.AddToInitialContext ?? null,
                isAddToInitialContextOverride: x.Options?.AddToInitialContext != null,
                query: x.Query,
                parametersSchema: x.ParametersSchema ?? "",
                parametersSampleObject: x.ParametersSampleObject ?? "",
                canRegenerateSchema: false,
                isEditing: false,
            })) ?? [],
        actions:
            dto.Actions?.map((x) => ({
                name: x.Name,
                description: x.Description,
                parametersSchema: x.ParametersSchema ?? "",
                parametersSampleObject: x.ParametersSampleObject ?? "",
                canRegenerateSchema: false,
                isEditing: false,
            })) ?? [],
        trimming: {
            method: getTrimmingMethod(dto),
            isEnableHistory: !!dto.ChatTrimming?.History,
            isSetHistoryExpiration: !!dto.ChatTrimming?.History?.HistoryExpirationInSec,
            historyExpirationInSeconds: dto.ChatTrimming?.History?.HistoryExpirationInSec,
            // messagesLengthBeforeTruncate: dto.ChatTrimming?.Truncate?.MessagesLengthBeforeTruncate,
            // messagesLengthAfterTruncate: dto.ChatTrimming?.Truncate?.MessagesLengthAfterTruncate,
            maxTokensBeforeSummarization: dto.ChatTrimming?.Tokens?.MaxTokensBeforeSummarization,
            maxTokensAfterSummarization: dto.ChatTrimming?.Tokens?.MaxTokensAfterSummarization,
        },
        subAgents:
            dto.SubAgents?.map((x) => ({
                identifier: x.Identifier,
                description: x.Description,
                isEditing: false,
            })) ?? [],
    };
}

function getTrimmingMethod(
    dto?: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration
): AiAgentTrimmingMethod {
    if (dto.ChatTrimming?.Tokens) {
        return "Tokens";
    }
    // if (dto.ChatTrimming?.Truncate) {
    //     return "Truncate";
    // }
    return null;
}

function mapToDto(formData: EditAiAgentFormData): Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration {
    return {
        Name: formData.name,
        Identifier: formData.identifier,
        ConnectionStringName: formData.connectionStringName,
        SystemPrompt: formData.systemPrompt,
        Disabled: formData.state === "Disabled",
        OutputSchema: formData.outputSchema,
        SampleObject: formData.sampleObject,
        Parameters:
            formData.parameters?.map((x) => ({
                Name: x.name,
                Description: x.description,
                SendToModel: x.isSendToModel,
                Policy: x.policy,
                Type: x.type,
            })) ?? [],
        Queries:
            formData.queries?.map((x) => ({
                Name: x.name,
                Description: x.description,
                Query: x.query,
                ParametersSampleObject: x.parametersSampleObject || null,
                ParametersSchema: x.parametersSchema || null,
                Options: {
                    AddToInitialContext: x.isAddToInitialContextOverride ? x.isAddToInitialContext : null,
                    AllowModelQueries: x.isAllowModelQueriesOverride ? x.isAllowModelQueries : null,
                },
            })) ?? [],
        Actions:
            formData.actions?.map((x) => ({
                Name: x.name,
                Description: x.description,
                ParametersSampleObject: x.parametersSampleObject || null,
                ParametersSchema: x.parametersSchema || null,
            })) ?? [],
        MaxModelIterationsPerCall: null,
        ChatTrimming:
            formData.trimming?.method != null
                ? {
                      History: formData.trimming.isEnableHistory
                          ? {
                                HistoryExpirationInSec: formData.trimming.isSetHistoryExpiration
                                    ? formData.trimming.historyExpirationInSeconds
                                    : null,
                            }
                          : null,
                      Tokens:
                          formData.trimming.method === "Tokens"
                              ? {
                                    MaxTokensBeforeSummarization: formData.trimming.maxTokensBeforeSummarization,
                                    MaxTokensAfterSummarization: formData.trimming.maxTokensAfterSummarization,
                                    ResultPrefix: null,
                                    SummarizationTaskBeginningPrompt: null,
                                    SummarizationTaskEndPrompt: null,
                                }
                              : null,
                      //   Truncate:
                      //       formData.trimming.method === "Truncate"
                      //           ? {
                      //                 MessagesLengthBeforeTruncate: formData.trimming.messagesLengthBeforeTruncate,
                      //                 MessagesLengthAfterTruncate: formData.trimming.messagesLengthAfterTruncate,
                      //             }
                      //           : null,
                  }
                : null,
        SubAgents:
            formData.subAgents?.map((x) => ({
                Identifier: x.identifier,
                Description: x.description,
            })) ?? [],
    };
}

export const editAiAgentUtils = {
    mapFromDto,
    mapToDto,
};
