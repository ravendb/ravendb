using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public abstract class AbstractAiSettings : IDynamicJsonValueConvertible
{
    public abstract void ValidateFields(List<string> errors);

    public abstract AiSettingsCompareDifferences Compare(AbstractAiSettings other);

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
