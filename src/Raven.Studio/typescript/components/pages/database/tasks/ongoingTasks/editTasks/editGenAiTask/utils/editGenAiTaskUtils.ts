import { EditGenAiTaskFormData } from "./editGenAiTaskValidation";

const getDefaultValues = (dto: Raven.Client.Documents.Operations.OngoingTasks.GenAi): EditGenAiTaskFormData => {
    if (!dto) {
        return {
            name: "",
            identifier: "",
            state: "Enabled",
            isSetResponsibleNode: false,
            responsibleNode: null,
            isPinResponsibleNode: false,
            connectionStringName: "",
            isAllowEtlOnNonEncryptedChannel: false,
            collectionName: "",
            maxConcurrency: null,
            prompt: "",
            schemaProvider: null,
            jsonSchema: "",
            sampleObject: "",
            updateScript: "",
            isResetScript: false,
            scriptToReset: null,
            script: "",
            documentId: "",
            playgroundContexts: [],
            playgroundModelOutputs: [],
            playgroundDocument: "",
            isForceSendingCachedObjects: false,
        };
    }

    return {
        name: dto.Configuration.Name,
        identifier: dto.Configuration.Identifier,
        state: dto.TaskState,
        isSetResponsibleNode: dto.MentorNode != null,
        responsibleNode: dto.MentorNode ?? null,
        isPinResponsibleNode: dto.PinToMentorNode,
        connectionStringName: dto.ConnectionStringName,
        isAllowEtlOnNonEncryptedChannel: dto.Configuration.AllowEtlOnNonEncryptedChannel,
        collectionName: dto.Configuration.Collection,
        maxConcurrency: dto.Configuration.MaxConcurrency,
        prompt: dto.Configuration.Prompt ?? "",
        schemaProvider: dto.Configuration.JsonSchema ? "jsonSchema" : "sampleObject",
        jsonSchema: dto.Configuration.JsonSchema ?? "",
        sampleObject: dto.Configuration.SampleObject ?? "",
        updateScript: dto.Configuration.UpdateScript ?? "",
        isResetScript: true,
        scriptToReset: dto.Configuration.Transforms?.[0].Name ?? null,
        script: dto.Configuration.GenAiTransformation?.Script ?? "",
        documentId: "",
        playgroundContexts: [],
        playgroundModelOutputs: [],
        playgroundDocument: "",
        isForceSendingCachedObjects: false,
    };
};

const mapToDto = (
    data: EditGenAiTaskFormData,
    taskId: number
): Raven.Client.Documents.Operations.AI.GenAiConfiguration => {
    return {
        TaskId: taskId,
        Name: data.name,
        Identifier: data.identifier,
        EtlType: "GenAi",
        ConnectionStringName: data.connectionStringName,
        AllowEtlOnNonEncryptedChannel: data.isAllowEtlOnNonEncryptedChannel,
        MaxConcurrency: data.maxConcurrency || undefined,
        Disabled: data.state === "Disabled",
        MentorNode: data.isSetResponsibleNode ? data.responsibleNode : undefined,
        PinToMentorNode: data.isSetResponsibleNode && data.isPinResponsibleNode,
        Transforms: null,
        Collection: data.collectionName,
        Prompt: data.prompt,
        JsonSchema: data.schemaProvider === "jsonSchema" ? data.jsonSchema : null,
        SampleObject: data.schemaProvider === "sampleObject" ? data.sampleObject : null,
        UpdateScript: data.updateScript,
        GenAiTransformation: {
            Script: data.script,
        },
    };
};

export const editGenAiTaskUtils = {
    getDefaultValues,
    mapToDto,
    defaultMaxConcurrency: 4,
};
