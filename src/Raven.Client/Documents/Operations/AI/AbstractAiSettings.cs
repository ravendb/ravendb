using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Base class for AI provider settings. Concrete providers should implement
/// validation and comparison logic and can extend the JSON serialization.
/// </summary>
public abstract class AbstractAiSettings : IDynamicJson
{
    /// <summary>
    /// Validates provider-specific fields, adding messages to the supplied error list.
    /// </summary>
    public virtual void ValidateFields(List<string> errors)
    {
        if (EmbeddingsMaxConcurrentBatches is <= 0)
            errors.Add($"Value of `{nameof(EmbeddingsMaxConcurrentBatches)}` field must be a positive number.");
    }

    /// <summary>
    /// Compares this settings instance with another of the same provider type and
    /// returns flags indicating the nature of the differences.
    /// </summary>
    public abstract AiSettingsCompareDifferences Compare(AbstractAiSettings other);

    /// <summary>
    /// Converts the settings into a JSON representation.
    /// </summary>
    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(EmbeddingsMaxConcurrentBatches)] = EmbeddingsMaxConcurrentBatches,
        };
    }

    /// <summary>
    /// Maximum number of query embedding batches that can be processed concurrently.
    /// Allow users to override the database global value 
    /// </summary>
    public int? EmbeddingsMaxConcurrentBatches { get; set; }
}
