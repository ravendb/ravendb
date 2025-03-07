using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;

namespace Raven.Server.Config.Categories;

[ConfigurationCategory(ConfigurationCategoryType.Ai)]
public sealed class AiConfiguration : ConfigurationCategory
{
    [Description("Maximum number of documents processed in a single batch by Embeddings Generation task. Higher values may improve throughput but require more resources and higher limits in AI service.")]
    [DefaultValue(128)]
    [ConfigurationEntry("Ai.Embeddings.Generation.MaxBatchSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int? EmbeddingsGenerationMaxBatchSize { get; set; }

    [Description("Maximum number of embeddings generated for queries to be cached in a single batch")]
    [DefaultValue(128)]
    [ConfigurationEntry("Ai.Embeddings.Generation.Querying.MaxCacheBatchSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int QueryEmbeddingsGenerationMaxCacheBatchSize { get; set; }

    [Description("Time in milliseconds to wait for additional requests before processing a batch of embedding requests. Lower values reduce latency but may decrease throughput.")]
    [DefaultValue(200)]
    [ConfigurationEntry("Ai.Embeddings.BatchTimeoutInMs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int BatchTimeoutInMs { get; set; }

    [Description("Maximum number of embedding requests to include in a single batch sent to the AI provider. Optimal values depend on the provider's rate limits and pricing model.")]
    [DefaultValue(128)]
    [ConfigurationEntry("Ai.Embeddings.MaxBatchSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int MaxBatchSize { get; set; }

    [Description("Maximum number of retry attempts for failed embedding generation requests before giving up. Retries use exponential backoff.")]
    [DefaultValue(3)]
    [ConfigurationEntry("Ai.Embeddings.MaxRetries", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int MaxRetries { get; set; }

    [Description("Base delay in milliseconds between retry attempts for failed embedding requests. Actual delay increases exponentially with each retry attempt. For example, with a base delay of 200ms, retries would wait 200ms, 400ms, 800ms, etc.")]
    [DefaultValue(200)]
    [TimeUnit(TimeUnit.Milliseconds)]
    [ConfigurationEntry("Ai.Embeddings.RetryDelayInMs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public TimeSetting RetryDelay { get; set; }
    
    [Description("Base delay for embedding generation. The delay increases exponentially with each retry attempt. E.g. 5s, 25s, 125s, etc.")]
    [DefaultValue(15)]
    [TimeUnit(TimeUnit.Seconds)]
    [ConfigurationEntry("Ai.Embeddings.TaskRetryDelayInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public TimeSetting TaskRetryDelay { get; set; }

    [Description("Maximum number of seconds ETL process will be in a fallback mode after a load connection failure to a destination. The fallback mode means suspending the process.")]
    [DefaultValue(60 * 15)]
    [TimeUnit(TimeUnit.Seconds)]
    [ConfigurationEntry("Ai.Embeddings.MaxFallbackTimeInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public TimeSetting MaxFallbackTime { get; set; }
    
    [Description("Maximum number of seconds ETL process will be in a fallback mode after a load connection failure to a destination. The fallback mode means suspending the process.")]
    [DefaultValue(EmbeddingsGenerationFallbackModeStrategy.Exponential)]
    [ConfigurationEntry("Ai.Embeddings.EmbeddingGenerationFallbackModeStrategy", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public EmbeddingsGenerationFallbackModeStrategy EmbeddingsGenerationFallbackModeStrategy { get; set; }
    
    [Description("Maximum number of embedding batches that can be processed concurrently. Controls the level of parallelism when sending requests to AI providers. Higher values improve throughput but increase resource usage and may trigger rate limits.")]
    [DefaultValue(4)]
    [ConfigurationEntry("Ai.Embeddings.MaxConcurrentBatches", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int MaxConcurrentBatches { get; set; }
}
