using Raven.AiAppliance.Agents;
using Raven.AiAppliance.Schema;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.AiAppliance.Raven;

/// Registers an agent for a given schema against an existing AI connection
/// string on the target per-app database. The connection string itself is
/// owned by the AI connection-strings endpoints (POST + lifecycle); this type
/// only handles agent creation. CreateAgentAsync is an upsert server-side, so
/// the operation is safe to re-run.
public static class AiAgentRegistrar
{
    public sealed record RegisterResult(string ConnectionStringName, string AgentIdentifier);

    public static async Task<RegisterResult> RegisterAsync(
        IDocumentStore store,
        IAgentSchema schema,
        string connectionStringName,
        string targetDatabase,
        CancellationToken ct = default)
    {
        var agent = BuildAgent(schema, connectionStringName);
        await store.AI.ForDatabase(targetDatabase)
            .CreateAgentAsync(agent, schema.AnswerSample, ct);

        return new RegisterResult(
            ConnectionStringName: connectionStringName,
            AgentIdentifier:      schema.Identifier);
    }

    private static AiAgentConfiguration BuildAgent(IAgentSchema schema, string connectionStringName)
    {
        var agent = new AiAgentConfiguration(
            name:                 schema.DisplayName,
            connectionStringName: connectionStringName,
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
