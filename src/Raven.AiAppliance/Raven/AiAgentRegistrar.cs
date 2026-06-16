using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.AiAppliance.Raven;

/// Persists an operator-defined <see cref="AiAgentConfiguration"/> as a RavenDB
/// agent on the target per-app database. The connection string is owned by the
/// AI connection-strings endpoints; this type only handles agent creation.
/// CreateAgentAsync is an upsert server-side, so the operation is safe to re-run.
public static class AiAgentRegistrar
{
    public sealed record RegisterResult(string Identifier);

    // Minimal default output shape so the smallest wizard body (name + prompt +
    // connection string) still provisions: RavenDB's AddOrUpdateAiAgentOperation
    // requires either OutputSchema or SampleObject to be non-empty.
    private const string DefaultSampleObject = """{"reply":""}""";

    /// <summary>
    /// Applies the minimal default output shape when the configuration declares neither a
    /// sample object nor an output schema. Both provisioning and the draft "Test agent" turn
    /// go through RavenDB's agent validation, which requires one — so both call this first.
    /// </summary>
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
}
