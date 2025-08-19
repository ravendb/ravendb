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

    [Description("Instruction text prepended to the serialized conversation when requesting a summary.")]
    [DefaultValue(@"Summarize the following AI conversation into a concise, linear narrative that retains all critical information. Ensure the summary:
- Includes key identifiers, usernames, timestamps, and any reference codes
- Preserves the original intent of both the user and the assistant in each exchange
- Reflects decisions made, suggestions given, preferences expressed, and any changes in direction
- Captures tone when relevant (e.g., sarcastic, formal, humorous, concerned)
- Omits general filler or small talk unless it contributes to context or tone Format the output in a structured manner (such as bullet points or labeled sections) suitable for fitting into a limited context window. Do not discard any information that contributes to understanding the conversation's flow and outcome.")]
    [ConfigurationEntry("Ai.Agent.Trimming.Summarization.SummarizationTaskBeginningPrompt", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public string SummarizationTaskBeginningPrompt { get; set; }

    [Description("The user-role message that triggers the conversation summarization process.")]
    [DefaultValue("Reminder - go over the entire previous conversation and summarize that according to the original instructions")]
    [ConfigurationEntry("Ai.Agent.Trimming.Summarization.SummarizationTaskEndPrompt", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public string SummarizationTaskEndPrompt { get; set; }


    [Description("The text prefix that appears before the generated summary of the previous conversation.")]
    [DefaultValue("Summary of previous conversation: ")]
    [ConfigurationEntry("Ai.Agent.Trimming.Summarization.SummarizationResultPrefix", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public string SummarizationResultPrefix { get; set; }

}
