namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Specifies the AI provider configured in an <see cref="AiConnectionString"/>.
/// </summary>
public enum AiConnectorType
{
    /// <summary>No provider configured.</summary>
    None,
    /// <summary>OpenAI (platform.openai.com).</summary>
    OpenAi,
    /// <summary>Azure OpenAI (Microsoft Azure Cognitive Services).</summary>
    AzureOpenAi,
    /// <summary>Ollama (self-hosted local models).</summary>
    Ollama,
    /// <summary>Embedded ONNX service (server-side, managed by RavenDB).</summary>
    Embedded,
    /// <summary>Google AI (e.g., Gemini embeddings).</summary>
    Google,
    /// <summary>Hugging Face Inference API.</summary>
    HuggingFace,
    /// <summary>Mistral AI.</summary>
    MistralAi
}
