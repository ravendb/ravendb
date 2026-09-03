using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;

namespace Raven.Quill.Raven;

public static class AiAgentRegistrar
{
    public sealed record RegisterResult(string Identifier);

    private const string DefaultSampleObject = """{"reply":""}""";

    // Raven's agent op requires OutputSchema or SampleObject; seed a minimal one
    public static void EnsureDefaultOutputShape(AiAgentConfiguration config)
    {
        if (string.IsNullOrWhiteSpace(config.SampleObject) && string.IsNullOrWhiteSpace(config.OutputSchema))
            config.SampleObject = DefaultSampleObject;
    }

    public static async Task<RegisterResult> RegisterAsync(
        IDocumentStore store,
        AiAgentConfiguration config,
        string targetDatabase,
        CancellationToken ct = default)
    {
        EnsureDefaultOutputShape(config);

        var result = await store.AI.ForDatabase(targetDatabase).CreateAgentAsync(config, ct);
        return new RegisterResult(result.Identifier);
    }

    public static async Task RegisterBindingsAsync(
        IDocumentStore store, string database, string agentId,
        Dictionary<string, WebhookBinding>? bindings, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        var id = AgentActionBindings.IdFor(agentId);

        if (bindings is not { Count: > 0 })
        {
            session.Delete(id);
        }
        else
        {
            var doc = new AgentActionBindings
            {
                Bindings = bindings
            };
            await session.StoreAsync(doc, id, ct);
        }

        await session.SaveChangesAsync(ct);
    }
}
