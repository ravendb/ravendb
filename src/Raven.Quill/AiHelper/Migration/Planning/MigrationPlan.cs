using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.Contracts;
using Raven.Quill.Wizard;

namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// What the model registered in a conversation, kept between the requests of one wizard session:
/// every turn and the final apply are separate calls, and each needs the plan as it stands.
/// </summary>
public sealed class MigrationPlan
{
    private Dictionary<string, PlanEntry>? _index;

    public string ConversationId { get; set; } = string.Empty;

    // A fresh instance per document: the store fills an existing value in place when loading, so a
    // shared default would carry one plan's conventions into every plan loaded after it.
    public NamingConventions Conventions { get; set; } = new();

    public List<PlanEntry> Entries { get; set; } = [];

    public SelectedSourceTable[]? SelectedTables { get; set; }

    public List<string> PendingUserRemovals { get; set; } = [];

    public async Task SaveAsync(IDocumentStore store, string slug, CancellationToken token = default)
    {
        if (_index is not null)
            Entries = _index.Values.ToList();

        using var session = store.OpenAsyncSession();
        session.Advanced.Patch<WizardState, MigrationPlan?>(WizardState.DocumentIdFor(slug), state => state.MigrationPlan, this);
        await session.SaveChangesAsync(token);
    }

    public void Upsert(string collection, string? rationale, CdcSinkTableConfig? config)
    {
        var index = Index();
        var version = index.TryGetValue(collection, out var existing) ? existing.Version + 1 : 1;

        index[collection] = new PlanEntry
        {
            Collection = collection,
            Rationale = rationale,
            Config = config,
            Version = version
        };
    }

    public void Remove(string collection) => Index().Remove(collection);

    public bool Contains(string collection) => Index().ContainsKey(collection);

    public void RemoveByUser(string collection)
    {
        Remove(collection);

        if (PendingUserRemovals.Contains(collection, StringComparer.OrdinalIgnoreCase) == false)
            PendingUserRemovals.Add(collection);
    }

    public List<PlanEntry> CurrentEntries() => Index().Values.ToList();

    public void SetConventions(NamingConventions conventions) => Conventions = conventions;

    private Dictionary<string, PlanEntry> Index() =>
        _index ??= Entries.ToDictionary(e => e.Collection, StringComparer.OrdinalIgnoreCase);
}
