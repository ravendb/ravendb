import genUtils from "common/generalUtils";
import { EditAiAgentFormData } from "./editAiAgentValidation";

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
            persistenceCollectionName: "",
            persistenceExpiresInSeconds: 2592000, // 30 days
            parameters: [],
            queries: [],
            actions: [],
            testPrompt: "",
            testParameters: [],
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
        persistenceCollectionName: dto.Persistence.Collection,
        persistenceExpiresInSeconds: dto.Persistence.Expires ? Number(dto.Persistence.Expires) / 1000 : null, // Expires is in milliseconds
        parameters:
            dto.Parameters?.map((x) => ({
                name: x,
            })) ?? [],
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
        testPrompt: "",
        testParameters:
            dto.Parameters?.map((x) => ({
                name: x,
                value: "",
            })) ?? [],
    };
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
            Collection: formData.persistenceCollectionName,
            Expires:
                isDocumentExpirationEnabled || formData.isEnableDocumentExpiration
                    ? genUtils.formatAsTimeSpan(formData.persistenceExpiresInSeconds * 1000)
                    : null,
        },
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
        Parameters: formData.parameters?.map((x) => x.name) ?? [],
    };
}

export const editAiAgentUtils = {
    mapFromDto,
    mapToDto,
};
