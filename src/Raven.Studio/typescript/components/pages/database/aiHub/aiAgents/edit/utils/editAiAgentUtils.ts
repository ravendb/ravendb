import { AiAgentTrimmingMethod, EditAiAgentFormData } from "./editAiAgentValidation";

function mapFromDto(
    dto?: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration,
    isClone?: boolean
): EditAiAgentFormData {
    if (!dto) {
        return {
            name: "",
            identifier: "",
            connectionStringName: "",
            systemPrompt: "",
            sampleObject: "",
            outputSchema: "",
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
        };
    }

    return {
        name: isClone ? "" : dto.Name,
        identifier: isClone ? "" : dto.Identifier,
        connectionStringName: dto.ConnectionStringName,
        systemPrompt: dto.SystemPrompt,
        sampleObject: dto.SampleObject,
        outputSchema: dto.OutputSchema,
        parameters:
            dto.Parameters?.map((x) => ({
                name: x.Name,
                description: x.Description,
            })) ?? [],
        queries:
            dto.Queries?.map((x) => ({
                name: x.Name,
                description: x.Description,
                isAllowModelQueries: x.Options?.AllowModelQueries ?? null,
                isAddToInitialContext: x.Options?.AddToInitialContext ?? null,
                query: x.Query,
                parametersSchema: x.ParametersSchema ?? "",
                parametersSampleObject: x.ParametersSampleObject ?? "",
                isSaved: true,
                isEditing: false,
            })) ?? [],
        actions:
            dto.Actions?.map((x) => ({
                name: x.Name,
                description: x.Description,
                parametersSchema: x.ParametersSchema ?? "",
                parametersSampleObject: x.ParametersSampleObject ?? "",
                isSaved: true,
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
        OutputSchema: formData.outputSchema,
        SampleObject: formData.sampleObject,
        Parameters:
            formData.parameters?.map((x) => ({
                Name: x.name,
                Description: x.description,
            })) ?? [],
        Queries:
            formData.queries?.map((x) => ({
                Name: x.name,
                Description: x.description,
                Query: x.query,
                ParametersSampleObject: x.parametersSampleObject || null,
                ParametersSchema: x.parametersSchema || null,
                Options: {
                    AddToInitialContext: x.isAddToInitialContext,
                    AllowModelQueries: x.isAllowModelQueries,
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
    };
}

export const editAiAgentUtils = {
    mapFromDto,
    mapToDto,
};
