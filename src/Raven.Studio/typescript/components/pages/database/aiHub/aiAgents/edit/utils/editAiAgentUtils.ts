import genUtils from "common/generalUtils";
import { AiAgentTrimmingMethod, EditAiAgentFormData } from "./editAiAgentValidation";
import { TimeInSeconds } from "common/constants/timeInSeconds";

function mapFromDto(
    dto?: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration,
    isClone?: boolean,
    isDocumentExpirationEnabled?: boolean
): EditAiAgentFormData {
    if (!dto) {
        return {
            name: "",
            identifier: "",
            connectionStringName: "",
            systemPrompt: "",
            sampleObject: "",
            outputSchema: "",
            isEnableDocumentExpiration: !isDocumentExpirationEnabled,
            persistenceConversationIdPrefix: "",
            persistenceExpiresInSeconds: TimeInSeconds.Day * 30,
            parameterInput: "",
            parameters: [],
            isToolsAdvancedSettings: false,
            queries: [],
            actions: [],
            maxModelIterationsPerCall: null,
            trimming: {
                method: null,
                historyExpirationInSeconds: null,
                messagesLengthBeforeTruncate: null,
                messagesLengthAfterTruncate: null,
                maxTokensBeforeSummarization: null,
                maxTokensAfterSummarization: null,
                resultPrefix: "",
                summarizationTaskBeginningPrompt: "",
                summarizationTaskEndPrompt: "",
            },
            test: {
                prompt: "",
                parameters: [],
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
        isEnableDocumentExpiration: !isDocumentExpirationEnabled,
        persistenceConversationIdPrefix: dto.Persistence.ConversationIdPrefix,
        persistenceExpiresInSeconds: dto.Persistence.Expires ? Number(dto.Persistence.Expires) / 1000 : null,
        parameterInput: "",
        parameters:
            dto.Parameters?.map((x) => ({
                name: x,
            })) ?? [],
        isToolsAdvancedSettings: dto.MaxModelIterationsPerCall != null,
        queries:
            dto.Queries?.map((x) => ({
                name: x.Name,
                description: x.Description,
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
        maxModelIterationsPerCall: dto.MaxModelIterationsPerCall,
        trimming: {
            method: getTrimmingMethod(dto),
            historyExpirationInSeconds: dto.ChatTrimming?.History?.HistoryExpiration
                ? Number(dto.ChatTrimming.History.HistoryExpiration) / 1000
                : null,
            messagesLengthBeforeTruncate: dto.ChatTrimming?.Truncate?.MessagesLengthBeforeTruncate,
            messagesLengthAfterTruncate: dto.ChatTrimming?.Truncate?.MessagesLengthAfterTruncate,
            maxTokensBeforeSummarization: dto.ChatTrimming?.Tokens?.MaxTokensBeforeSummarization,
            maxTokensAfterSummarization: dto.ChatTrimming?.Tokens?.MaxTokensAfterSummarization,
            resultPrefix: dto.ChatTrimming?.Tokens?.ResultPrefix,
            summarizationTaskBeginningPrompt: dto.ChatTrimming?.Tokens?.SummarizationTaskBeginningPrompt,
            summarizationTaskEndPrompt: dto.ChatTrimming?.Tokens?.SummarizationTaskEndPrompt,
        },
        test: {
            prompt: "",
            parameters:
                dto.Parameters?.map((x) => ({
                    name: x,
                    value: "",
                })) ?? [],
        },
    };
}

function getTrimmingMethod(
    dto?: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration
): AiAgentTrimmingMethod {
    if (dto.ChatTrimming?.Tokens) {
        return "Tokens";
    }
    if (dto.ChatTrimming?.Truncate) {
        return "Truncate";
    }
    return null;
}

function mapToDto(
    formData: EditAiAgentFormData,
    isDocumentExpirationEnabled?: boolean
): Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration {
    return {
        Name: formData.name,
        Identifier: formData.identifier,
        ConnectionStringName: formData.connectionStringName,
        SystemPrompt: formData.systemPrompt,
        OutputSchema: formData.outputSchema,
        SampleObject: formData.sampleObject,
        Persistence: {
            ConversationIdPrefix: formData.persistenceConversationIdPrefix,
            Expires:
                isDocumentExpirationEnabled || formData.isEnableDocumentExpiration
                    ? genUtils.formatAsTimeSpan(formData.persistenceExpiresInSeconds * 1000)
                    : null,
        },
        Parameters: formData.parameters?.map((x) => x.name) ?? [],
        Queries:
            formData.queries?.map((x) => ({
                Name: x.name,
                Description: x.description,
                Query: x.query,
                ParametersSampleObject: x.parametersSampleObject || null,
                ParametersSchema: x.parametersSchema || null,
            })) ?? [],
        Actions:
            formData.actions?.map((x) => ({
                Name: x.name,
                Description: x.description,
                ParametersSampleObject: x.parametersSampleObject || null,
                ParametersSchema: x.parametersSchema || null,
            })) ?? [],
        MaxModelIterationsPerCall: formData.maxModelIterationsPerCall,
        ChatTrimming:
            formData.trimming?.method != null
                ? {
                      History: formData.trimming.historyExpirationInSeconds
                          ? {
                                HistoryExpiration: genUtils.formatAsTimeSpan(
                                    formData.trimming.historyExpirationInSeconds * 1000
                                ),
                            }
                          : null,
                      Tokens:
                          formData.trimming.method === "Tokens"
                              ? {
                                    MaxTokensBeforeSummarization: formData.trimming.maxTokensBeforeSummarization,
                                    MaxTokensAfterSummarization: formData.trimming.maxTokensAfterSummarization,
                                    ResultPrefix: formData.trimming.resultPrefix,
                                    SummarizationTaskBeginningPrompt:
                                        formData.trimming.summarizationTaskBeginningPrompt,
                                    SummarizationTaskEndPrompt: formData.trimming.summarizationTaskEndPrompt,
                                }
                              : null,
                      Truncate:
                          formData.trimming.method === "Truncate"
                              ? {
                                    MessagesLengthBeforeTruncate: formData.trimming.messagesLengthBeforeTruncate,
                                    MessagesLengthAfterTruncate: formData.trimming.messagesLengthAfterTruncate,
                                }
                              : null,
                  }
                : null,
    };
}

export const editAiAgentUtils = {
    mapFromDto,
    mapToDto,
};
