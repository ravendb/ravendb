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
            isStartingPoint: false,
            startingPointType: "Beginning of Time",
            startingPointChangeVector: "",
            prompt: "",
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
        isStartingPoint: false,
        startingPointType: "Beginning of Time",
        startingPointChangeVector: "",
        prompt: dto.Configuration.Prompt ?? "",
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
        JsonSchema: data.jsonSchema,
        SampleObject: data.sampleObject,
        UpdateScript: data.updateScript,
        GenAiTransformation: {
            Script: data.script,
        },
    };
};

const getSerializedChangeVector = (data: EditGenAiTaskFormData, taskId: number): string => {
    let changeVector = taskId ? "DoNotChange" : "BeginningOfTime";

    if (data.isStartingPoint) {
        switch (data.startingPointType) {
            case "Beginning of Time":
                changeVector = "BeginningOfTime";
                break;
            case "Latest Document":
                changeVector = "LastDocument";
                break;
            case "Change Vector":
                changeVector = data.startingPointChangeVector?.trim().replace(/\r?\n/g, " ") ?? "";
                break;
        }
    }
    return changeVector;
};

export const editGenAiTaskUtils = {
    getDefaultValues,
    mapToDto,
    getSerializedChangeVector,
    defaultMaxConcurrency: 4,
};
