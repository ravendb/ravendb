using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Client.Documents.Operations.AI;

public class EmbeddingsTransformation
{
    internal const string GenerateEmbeddingsFunctionName = "embeddings.generate";

    private static readonly Regex EmbeddingsGenerateRegex = new Regex(GenerateEmbeddingsFunctionName, RegexOptions.Compiled);

    public string Script { get; set; }

    public ChunkingOptions ChunkingOptions { get; set; } = new() { ChunkingMethod = ChunkingMethod.PlainTextSplit, MaxTokensPerChunk = 256 };

    internal void Validate(List<string> errors)
    {
        ValidateScript(errors);
        
        ChunkingOptions.Validate(GenerateEmbeddingsFunctionName, errors);
    }

    private void ValidateScript(List<string> errors)
    {
        var match = EmbeddingsGenerateRegex.Match(Script);
        
        if (match.Length == 0)
            errors.Add($"Transformation script must use {GenerateEmbeddingsFunctionName} method.");
    }

    internal static bool AreEqual(EmbeddingsTransformation left, EmbeddingsTransformation right)
    {
        if (left == null && right == null)
            return true;
        
        if (left == null || right == null)
            return false;
        
        return left.Script == right.Script &&
               ChunkingOptions.AreEqual(left.ChunkingOptions, right.ChunkingOptions);
    }
}
