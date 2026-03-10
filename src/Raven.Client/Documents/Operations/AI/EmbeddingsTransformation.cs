using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Describes a JavaScript transformation used to generate embeddings from documents.
/// </summary>
public class EmbeddingsTransformation
{
    internal const string GenerateEmbeddingsFunctionName = "embeddings.generate";

    private static readonly Regex EmbeddingsGenerateRegex = new Regex(GenerateEmbeddingsFunctionName, RegexOptions.Compiled);

    /// <summary>
    /// The JavaScript script that calls <c>embeddings.generate(...)</c> to produce vector embeddings.
    /// </summary>
    public string Script { get; set; }

    /// <summary>
    /// Chunking behavior to apply to the text prior to generating embeddings.
    /// </summary>
    public ChunkingOptions ChunkingOptions { get; set; } = new() { ChunkingMethod = ChunkingMethod.PlainTextSplit, MaxTokensPerChunk = 256 };

    internal void Validate(List<string> errors)
    {
        ValidateScript(errors);
        
        ChunkingOptions.Validate(GenerateEmbeddingsFunctionName, errors);
    }

    /// <summary>
    /// Validates that the script contains at least one call to <c>embeddings.generate</c>.
    /// </summary>
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

        return left.Equals(right);
    }

    private bool Equals(EmbeddingsTransformation other)
    {
        return Script == other.Script && 
               ChunkingOptions.AreEqual(ChunkingOptions, other.ChunkingOptions);
    }

    public override bool Equals(object obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;
        return Equals((EmbeddingsTransformation)obj);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Script, ChunkingOptions);
    }
}
