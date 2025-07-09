import genUtils from "common/generalUtils";
import { EditAiAgentFormData } from "./editAiAgentValidation";

function mapFromDto(
    name: string,
    dto?: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration,
    isClone?: boolean
): EditAiAgentFormData {
    if (!name) {
        return {
            name: "",
            connectionStringName: "",
            systemPrompt: "",
            outputSchema: "",
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
        name: isClone ? "" : name,
        connectionStringName: dto.ConnectionStringName,
        systemPrompt: dto.SystemPrompt,
        outputSchema: dto.OutputSchema,
        persistenceCollectionName: dto.Persistence.Collection,
        persistenceExpiresInSeconds: genUtils.timeSpanToSeconds(dto.Persistence.Expires),
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

function mapToDto(formData: EditAiAgentFormData): Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration {
    return {
        ConnectionStringName: formData.connectionStringName,
        SystemPrompt: formData.systemPrompt,
        OutputSchema: formData.outputSchema,
        SampleObject: formData.sampleObject,
        Persistence: {
            Collection: formData.persistenceCollectionName,
            Expires: genUtils.formatAsTimeSpan(formData.persistenceExpiresInSeconds * 1000),
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
