using System.Text.RegularExpressions;

namespace Raven.Client.Documents.Operations.AI;

public class EmbeddingsTransformation
{
    internal const string GenerateEmbeddingsFunctionName = "embeddings.generate";

    private static readonly Regex EmbeddingsGenerateRegex = new Regex(GenerateEmbeddingsFunctionName, RegexOptions.Compiled);

    public string Script { get; set; }
}
