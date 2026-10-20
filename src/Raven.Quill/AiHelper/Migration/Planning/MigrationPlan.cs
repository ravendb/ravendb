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

    public List<UserEdit> PendingUserEdits { get; set; } = [];

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

    /// <summary>
    /// The planner rebuilt or removed the collection, so what the user did to it is no longer what the plan holds.
    /// A rebuild also ends the user's removal; a removal by the planner leaves it.
    /// </summary>
    public void ForgetUserEdits(string collection, bool includingRemoval) =>
        PendingUserEdits.RemoveAll(edit => (includingRemoval || edit.Kind != UserEditKind.Removed) &&
                                           string.Equals(edit.Collection, collection, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Applies an operator's change: a null <paramref name="config"/> deletes the collection, a config naming
    /// another collection renames it, and a collection the plan does not hold is added. Returns why the change
    /// was refused, or null.
    /// </summary>
    public string? ApplyUserEdit(string collection, CdcSinkTableConfig? config)
    {
        var index = Index();

        if (config is null)
        {
            Remove(collection);
            RecordRemoval(collection);
            return null;
        }

        var name = string.IsNullOrWhiteSpace(config.CollectionName) ? collection : config.CollectionName;
        config.CollectionName = name;

        if (index.TryGetValue(collection, out var existing) == false)
        {
            if (index.ContainsKey(name))
                return $"the plan already has a collection named {name}";

            Upsert(name, rationale: null, config);
            RecordAddition(name);
            return null;
        }

        if (string.Equals(name, collection, StringComparison.Ordinal))
        {
            Upsert(collection, existing.Rationale, config);
            RecordEdit(collection);
            return null;
        }

        if (string.Equals(name, collection, StringComparison.OrdinalIgnoreCase) == false && index.ContainsKey(name))
            return $"the plan already has a collection named {name}";

        Remove(collection);
        Upsert(name, existing.Rationale, config);
        RecordRename(collection, name);
        return null;
    }

    /// <summary>The collections that still link to <paramref name="collection"/>, which is no longer in the plan.</summary>
    public string[] LinkingTo(string collection) =>
        Index().Values
            .Where(entry => entry.Config is not null && LinkedCollections(entry.Config).Contains(collection, StringComparer.OrdinalIgnoreCase))
            .Select(entry => entry.Collection)
            .ToArray();

    private static List<string?> LinkedCollections(CdcSinkTableConfig config)
    {
        var linked = (config.LinkedTables ?? []).Select(link => link.LinkedCollectionName).ToList();
        CdcSinkConfiguration.ForEachEmbeddedTable(config.EmbeddedTables, embedded =>
            linked.AddRange((embedded.LinkedTables ?? []).Select(link => link.LinkedCollectionName)));
        return linked;
    }

    private int PendingIndexOf(string collection) =>
        PendingUserEdits.FindIndex(edit => string.Equals(edit.Collection, collection, StringComparison.OrdinalIgnoreCase));

    /// <summary>The record of the collection as it stands now, not an earlier removal of the same name.</summary>
    private int LivePendingIndexOf(string collection) =>
        PendingUserEdits.FindIndex(edit => edit.Kind != UserEditKind.Removed &&
                                           string.Equals(edit.Collection, collection, StringComparison.OrdinalIgnoreCase));

    private void RecordRemoval(string collection)
    {
        var pending = LivePendingIndexOf(collection);
        if (pending >= 0)
        {
            var previous = PendingUserEdits[pending];
            PendingUserEdits.RemoveAt(pending);

            if (previous.Kind == UserEditKind.Added)
                return;

            if (previous is { Kind: UserEditKind.Renamed, PreviousName: not null })
                collection = previous.PreviousName;
        }

        if (PendingUserEdits.Contains(new UserEdit(collection, UserEditKind.Removed)) == false)
            PendingUserEdits.Add(new UserEdit(collection, UserEditKind.Removed));
    }

    private void RecordEdit(string collection)
    {
        if (LivePendingIndexOf(collection) < 0)
            PendingUserEdits.Add(new UserEdit(collection, UserEditKind.Edited));
    }

    private void RecordAddition(string collection)
    {
        var pending = PendingIndexOf(collection);
        if (pending >= 0 && PendingUserEdits[pending].Kind == UserEditKind.Removed)
        {
            PendingUserEdits[pending] = new UserEdit(collection, UserEditKind.Edited);
            return;
        }

        if (pending < 0)
            PendingUserEdits.Add(new UserEdit(collection, UserEditKind.Added));
    }

    private void RecordRename(string from, string to)
    {
        var pending = LivePendingIndexOf(from);
        var previous = pending >= 0 ? PendingUserEdits[pending] : null;
        if (pending >= 0)
            PendingUserEdits.RemoveAt(pending);

        var original = previous switch
        {
            { Kind: UserEditKind.Added } => null,
            { Kind: UserEditKind.Renamed } => previous.PreviousName,
            _ => from
        };

        PendingUserEdits.Add(original switch
        {
            null => new UserEdit(to, UserEditKind.Added),
            _ when string.Equals(original, to, StringComparison.Ordinal) => new UserEdit(to, UserEditKind.Edited),
            _ => new UserEdit(to, UserEditKind.Renamed, original)
        });
    }

    public List<PlanEntry> CurrentEntries() => Index().Values.ToList();

    public void SetConventions(NamingConventions conventions) => Conventions = conventions;

    private Dictionary<string, PlanEntry> Index() =>
        _index ??= Entries.ToDictionary(e => e.Collection, StringComparer.OrdinalIgnoreCase);
}
