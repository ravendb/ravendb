namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Indicates the category of AI model used by a given connection string.
/// </summary>
public enum AiModelType
{
    /// <summary>Text embeddings generation models.</summary>
    TextEmbeddings,
    /// <summary>Chat/completions models.</summary>
    Chat
}
