function getHasWrittenSchema(jsonSchema: string): boolean {
    return !!jsonSchema?.trim();
}

function getWrittenSchemaOrNull(jsonSchema: string): string | null {
    return getHasWrittenSchema(jsonSchema) ? jsonSchema : null;
}

function getIsSchemaGeneratedByServer(sampleObject: string, jsonSchema: string): boolean {
    return !!sampleObject?.trim() && !getHasWrittenSchema(jsonSchema);
}

function getHasSampleObjectOrWrittenSchema(sampleObject: string, jsonSchema: string): boolean {
    return getIsSchemaGeneratedByServer(sampleObject, jsonSchema) || getHasWrittenSchema(jsonSchema);
}

function getIsStrictFromSaved(savedJsonSchema: string, savedIsStrict: boolean): boolean {
    const isGeneratedSchemaAlwaysStrict = !getHasWrittenSchema(savedJsonSchema);

    return isGeneratedSchemaAlwaysStrict || !!savedIsStrict;
}

function getIsStrictToSave(jsonSchema: string, isStrict: boolean): boolean {
    const isSetOnlyForWrittenSchema = getHasWrittenSchema(jsonSchema);

    return isSetOnlyForWrittenSchema && !!isStrict;
}

export const schemaStrictModeUtils = {
    getHasWrittenSchema,
    getWrittenSchemaOrNull,
    getIsSchemaGeneratedByServer,
    getHasSampleObjectOrWrittenSchema,
    getIsStrictFromSaved,
    getIsStrictToSave,
};
