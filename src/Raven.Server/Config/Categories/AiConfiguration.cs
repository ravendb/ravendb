using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;

namespace Raven.Server.Config.Categories;

[ConfigurationCategory(ConfigurationCategoryType.Ai)]
public sealed class AiConfiguration : ConfigurationCategory
{
    #region Embeddings Generation Task

    [Description("Maximum number of documents processed in a single batch by the Embeddings Generation task. " +
                 "Higher values may improve throughput but require more resources and higher limits from the AI service.")]
    [DefaultValue(128)]
    [ConfigurationEntry("Ai.Embeddings.Generation.Task.MaxBatchSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int? EmbeddingsGenerationTaskMaxBatchSize { get; set; }

    [Description("Base delay for Embedding Generation task retries. The actual wait time between retry attempts depends on the configured FallbackModeStrategy. " +
                 $"When using '{nameof(EmbeddingsGenerationRetryStrategy.Linear)}' strategy, the delay increases linearly (e.g., 15s, 30s, 45s). " +
                 $"When using '{nameof(EmbeddingsGenerationRetryStrategy.Exponential)}' strategy, the delay increases exponentially with each retry attempt (e.g. 15s, 60s, 120s, 240s).")]
    [DefaultValue(15)]
    [TimeUnit(TimeUnit.Seconds)]
    [ConfigurationEntry("Ai.Embeddings.Generation.Task.RetryDelayInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public TimeSetting EmbeddingsGenerationTaskRetryDelay { get; set; }

    [Description("Maximum number of seconds the Embeddings Generation task remains suspended (fallback mode) following a connection failure to the AI provider. " +
                 "After this time, the system retries automatically.")]
    [DefaultValue(60 * 15)]
    [TimeUnit(TimeUnit.Seconds)]
    [ConfigurationEntry("Ai.Embeddings.Generation.Task.MaxFallbackTimeInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public TimeSetting EmbeddingsGenerationTaskMaxFallbackTime { get; set; }

    [Description($"Strategy to use for retry intervals when embeddings generation fails. " +
                 $"'{nameof(EmbeddingsGenerationRetryStrategy.Linear)}' uses fixed intervals between retries, while " +
                 $"'{nameof(EmbeddingsGenerationRetryStrategy.Exponential)}' increases the wait time exponentially after each failure " +
                 $"(e.g., 15s, 30s, 60s for {nameof(EmbeddingsGenerationRetryStrategy.Linear)}; or 15s, 60s, 120s, 240s for {nameof(EmbeddingsGenerationRetryStrategy.Exponential)} with base 15s).")]
    [DefaultValue(EmbeddingsGenerationRetryStrategy.Exponential)]
    [ConfigurationEntry("Ai.Embeddings.Generation.Task.RetryStrategy", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public EmbeddingsGenerationRetryStrategy EmbeddingsGenerationTaskRetryStrategy { get; set; }

    #endregion

    #region Querying

    #region Caching

    [Description("Maximum number of embeddings generated from query terms during vector searches that can be stored in the embeddings cache collection in a single batch operation. " +
                 "Caching these embeddings reduces redundant processing and improves retrieval efficiency for identical queries.")]
    [DefaultValue(128)]
    [ConfigurationEntry("Ai.Embeddings.Generation.Querying.Caching.MaxBatchSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int QueryEmbeddingsGenerationMaxCacheBatchSize { get; set; }
    
    #endregion

    #region Batching

    [Description("Time in milliseconds to wait for additional query embedding requests before sending the current batch to the AI provider. " +
                 "Lower values decrease latency for query embedding generation but may reduce throughput.")]
    [DefaultValue(200)]
    [ConfigurationEntry("Ai.Embeddings.Generation.Querying.Batching.TimeoutInMs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int QueryEmbeddingsBatchTimeout { get; set; }

    [Description("Maximum number of query embedding requests to include in a single batch sent to the AI provider. Optimal values depend on the provider's rate limits and pricing model.")]
    [DefaultValue(128)]
    [ConfigurationEntry("Ai.Embeddings.Generation.Querying.Batching.MaxBatchSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int QueryEmbeddingsMaxBatchSize { get; set; }

    [Description("Maximum number of retry attempts for failed query embedding generation requests before giving up. Retries are performed using an exponential backoff strategy.")]
    [DefaultValue(3)]
    [ConfigurationEntry("Ai.Embeddings.Generation.Querying.Batching.MaxRetries", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int QueryEmbeddingsBatchMaxRetries { get; set; }

    [Description("Base delay in milliseconds between retry attempts for failed query embedding requests. Actual delay increases exponentially with each retry attempt. " +
                 "For example, with a base delay of 200ms, retries would occur after 200ms, 400ms, 800ms, etc.")]
    [DefaultValue(200)]
    [TimeUnit(TimeUnit.Milliseconds)]
    [ConfigurationEntry("Ai.Embeddings.Generation.Querying.Batching.RetryDelayInMs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public TimeSetting QueryEmbeddingsBatchRetryDelay { get; set; }

    [Description("Maximum number of query embedding batches that can be processed concurrently. Controls the degree of parallelism when sending query embedding requests to AI providers. " +
                 "Higher values improve throughput but increase resource usage and may trigger rate limits.")]
    [DefaultValue(4)]
    [ConfigurationEntry("Ai.Embeddings.Generation.Querying.Batching.MaxConcurrentBatches", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int QueryEmbeddingsMaxConcurrentBatches { get; set; }
    
    #endregion
    
    #endregion
}
