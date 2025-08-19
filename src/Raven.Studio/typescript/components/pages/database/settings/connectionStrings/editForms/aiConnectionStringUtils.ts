export const getConnectorType = (
    connection: Raven.Client.Documents.Operations.AI.AiConnectionString
): Raven.Client.Documents.Operations.AI.AiConnectorType => {
    if (connection.AzureOpenAiSettings) {
        return "AzureOpenAi";
    }
    if (connection.GoogleSettings) {
        return "Google";
    }
    if (connection.HuggingFaceSettings) {
        return "HuggingFace";
    }
    if (connection.OllamaSettings) {
        return "Ollama";
    }
    if (connection.EmbeddedSettings) {
        return "Embedded";
    }
    if (connection.OpenAiSettings) {
        return "OpenAi";
    }
    if (connection.MistralAiSettings) {
        return "MistralAi";
    }

    throw new Error("No connector type found. Please check the connection string.");
};

export function mapAiConnectionStringToSettingsDto(
    connection: Raven.Client.Documents.Operations.AI.AiConnectionString
): AiConnectionStringsSettings {
    const settings = [
        connection.AzureOpenAiSettings,
        connection.GoogleSettings,
        connection.HuggingFaceSettings,
        connection.OllamaSettings,
        connection.EmbeddedSettings,
        connection.OpenAiSettings,
        connection.MistralAiSettings,
    ].find(Boolean);

    if (!settings) {
        throw new Error("No settings found. Please check the connection string.");
    }

    return settings;
}
