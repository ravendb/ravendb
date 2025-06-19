using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

public class GenAiResultItem
{
    public List<string> DebugOutput { get; set; }

    public DynamicJsonValue DebugActions { get; set; }

    public ModelOutput ModelOutput { get; set; }

    public ContextOutput ContextOutput { get; set; }

    internal string DocId { get; set; }

    internal bool UpdateHash { get; set; } = true;

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(DebugOutput)] = DebugOutput == null ? null : new DynamicJsonArray(DebugOutput),
            [nameof(DebugActions)] = DebugActions,
            [nameof(ContextOutput)] = ContextOutput?.ToJson(),
            [nameof(ModelOutput)] = ModelOutput?.ToJson()
        };
    }
}

public class ModelOutput
{
    public BlittableJsonReaderObject Output { get; set; }
    public ModelUsageStats Usage { get; set; }

    public DynamicJsonValue ToJson() => new()
    {
        [nameof(Usage)] = Usage?.ToJson(), 
        [nameof(Output)] = Output
    };
}

public class ContextOutput
{
    public BlittableJsonReaderObject Context { get; set; }
    public bool IsCached { get; set; }
    public string AiHash { get; set; }

    public DynamicJsonValue ToJson() => new()
    {
        [nameof(Context)] = Context, 
        [nameof(IsCached)] = IsCached, 
        [nameof(AiHash)] = AiHash
    };
}

public class ModelUsageStats
{
    public int PromptTokens { get; set; }
    public int CompletionTokens { get; set; }
    public int TotalTokens { get; set; }

    public DynamicJsonValue ToJson() => new()
    {
        [nameof(PromptTokens)] = PromptTokens,
        [nameof(CompletionTokens)] = CompletionTokens,
        [nameof(TotalTokens)] = TotalTokens
    };
}
