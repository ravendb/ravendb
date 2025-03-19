using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;

namespace Raven.Server.Config.Categories;

[ConfigurationCategory(ConfigurationCategoryType.Ai)]
public sealed class AiConfiguration : ConfigurationCategory
{
    [Description("Maximum number of documents processed in a single batch by the Embeddings Generation task. " +
                 "Higher values may improve throughput but can increase latency and require more resources and higher limits from the Embeddings Generation service.")]
    [DefaultValue(128)]
    [ConfigurationEntry("Ai.Embeddings.MaxBatchSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int? EmbeddingsGenerationMaxBatchSize { get; set; }

    [Description("Maximum number of seconds the Embeddings Generation task remains suspended (fallback mode) following a connection failure to the AI provider. " +
                 "After this time, the system retries automatically.")]
    [DefaultValue(60 * 15)]
    [TimeUnit(TimeUnit.Seconds)]
    [ConfigurationEntry("Ai.Embeddings.MaxFallbackTimeInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public TimeSetting EmbeddingsGenerationMaxFallbackTime { get; set; }
    
    
    [Description("Maximum number of query embedding batches that can be processed concurrently. Controls the degree of parallelism when sending query embedding requests to AI providers. " +
                 "Higher values improve throughput but increase resource usage and may trigger rate limits.")]
    [DefaultValue(4)]
    [ConfigurationEntry("Ai.Embeddings.MaxConcurrentBatches", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    [MinValue(1)]
    public int EmbeddingsMaxConcurrentBatches { get; set; }

}
