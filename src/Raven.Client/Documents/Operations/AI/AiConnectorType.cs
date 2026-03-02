namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Specifies the AI provider configured in an <see cref="AiConnectionString"/>.
/// </summary>
public enum AiConnectorType
{
    /// <summary>No provider configured.</summary>
    None,
    /// <summary>OpenAI.</summary>
    OpenAi,
    /// <summary>Azure OpenAI.</summary>
    AzureOpenAi,
    /// <summary>Ollama.</summary>
    Ollama,
    /// <summary>Embedded ONNX service.</summary>
    Embedded,
    /// <summary>Google AI.</summary>
    Google,
    /// <summary>Hugging Face Inference API.</summary>
    HuggingFace,
    /// <summary>Mistral AI.</summary>
    MistralAi,
    /// <summary>Vertex AI.</summary>
    Vertex
}
