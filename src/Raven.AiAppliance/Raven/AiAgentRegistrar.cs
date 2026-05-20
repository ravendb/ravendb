using Raven.AiAppliance.Agents;
using Raven.AiAppliance.Hosting;
using Raven.AiAppliance.Schema;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;

namespace Raven.AiAppliance.Raven;

/// Registers (or refreshes) an AI connection string + an agent for a given schema.
/// Both server-side puts are upserts, so the operation is safe to re-run.
/// T-1 does not call this on startup; T-3's wizard will.
public static class AiAgentRegistrar
{
    public sealed record RegisterResult(
        string ConnectionStringName,
        string AgentIdentifier,
        string Provider,
        string Endpoint,
        string Model);

    public static async Task<RegisterResult> RegisterAsync(
        IDocumentStore store,
        IAgentSchema schema,
        ApplianceOptions options,
        CancellationToken ct = default)
    {
        var aiCs = BuildAiConnectionString(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(aiCs), ct);

        var agent = BuildAgent(schema, options);
        await store.AI.CreateAgentAsync(agent, schema.AnswerSample, ct);

        return new RegisterResult(
            ConnectionStringName: options.LlmConnectionStringName,
            AgentIdentifier:      schema.Identifier,
            Provider:             options.LlmProvider,
            Endpoint:             options.LlmEndpoint,
            Model:                options.LlmModel);
    }

    public static async Task<bool> UnregisterAsync(IDocumentStore store, string agentIdentifier, CancellationToken ct = default)
    {
        var existing = await store.AI.GetAgentAsync(agentIdentifier, ct);
        if (existing is null) return false;
        await store.AI.DeleteAgentAsync(agentIdentifier, ct);
        return true;
    }

    private static AiConnectionString BuildAiConnectionString(ApplianceOptions options)
    {
        var cs = new AiConnectionString
        {
            Name       = options.LlmConnectionStringName,
            Identifier = options.LlmConnectionStringName,
            ModelType  = AiModelType.Chat,
        };

        switch (options.LlmProvider.ToLowerInvariant())
        {
            case "openai":
                cs.OpenAiSettings = new OpenAiSettings(
                    apiKey:   options.LlmApiKey,
                    endpoint: options.LlmEndpoint,
                    model:    options.LlmModel);
                break;

            case "ollama":
                cs.OllamaSettings = new OllamaSettings(
                    uri:   options.LlmEndpoint,
                    model: options.LlmModel);
                break;

            default:
                throw new InvalidOperationException(
                    $"Unsupported LLM provider '{options.LlmProvider}'. Use 'openai' (covers OpenAI-compatible endpoints) or 'ollama'.");
        }

        return cs;
    }

    private static AiAgentConfiguration BuildAgent(IAgentSchema schema, ApplianceOptions options)
    {
        var agent = new AiAgentConfiguration(
            name:                 schema.DisplayName,
            connectionStringName: options.LlmConnectionStringName,
            systemPrompt:         schema.SystemPrompt)
        {
            Identifier = schema.Identifier,
        };

        foreach (var p in schema.Parameters)
        {
            agent.Parameters.Add(new AiAgentParameter(
                name:        p.Name,
                description: p.Description,
                sendToModel: p.SendToModel,
                policy:      Map(p.Policy)));
        }

        agent.Queries = schema.Queries.Select(q => new AiAgentToolQuery
        {
            Name                   = q.Name,
            Description            = q.Description,
            Query                  = q.Query,
            ParametersSampleObject = q.ParametersSampleJson,
            Options                = new AiAgentToolQueryOptions
            {
                AddToInitialContext = q.AddToInitialContext,
                AllowModelQueries   = q.AllowModelQueries,
            },
        }).ToList();

        agent.ChatTrimming = new AiAgentChatTrimmingConfiguration(
            new AiAgentSummarizationByTokens
            {
                MaxTokensBeforeSummarization = 8_192,
                MaxTokensAfterSummarization  = 1_024,
            },
            new AiAgentHistoryConfiguration(expiration: TimeSpan.FromDays(7)));

        return agent;
    }

    private static AiAgentParameterPolicy Map(AgentParameterPolicy policy) => policy switch
    {
        AgentParameterPolicy.Default               => AiAgentParameterPolicy.Default,
        AgentParameterPolicy.ForbidModelGeneration => AiAgentParameterPolicy.ForbidModelGeneration,
        _ => throw new ArgumentOutOfRangeException(nameof(policy), policy, null),
    };
}
