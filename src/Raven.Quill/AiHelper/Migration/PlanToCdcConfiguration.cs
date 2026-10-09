using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Endpoints;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// Assembles what the session registered into the configuration the rest of the wizard already
/// knows how to carry, and reports which of the discovered tables it does not cover.
/// </summary>
public static class PlanToCdcConfiguration
{
    /// <summary>
    /// Keeps the task and connection string names of the mapping the app already has. A new app has
    /// none yet, so it gets the same defaults the map endpoint gives a mapping saved without them.
    /// </summary>
    public static CdcSinkConfiguration Build(IEnumerable<PlanEntry> entries, CdcSinkConfiguration? previousMapping) =>
        Build(
            entries,
            string.IsNullOrWhiteSpace(previousMapping?.Name) ? WizardEndpoints.DefaultCdcTaskName : previousMapping.Name,
            string.IsNullOrWhiteSpace(previousMapping?.ConnectionStringName)
                ? WizardEndpoints.SourceConnectionStringName
                : previousMapping.ConnectionStringName);

    public static CdcSinkConfiguration Build(
        IEnumerable<PlanEntry> entries,
        string name,
        string connectionStringName) =>
        new()
        {
            Name = name,
            ConnectionStringName = connectionStringName,
            Tables = entries
                .Select(e => e.Config)
                .Where(c => c is not null)
                .Select(c => WithInheritedSchemas(c!))
                .ToList()
        };

    /// <summary>
    /// The planner leaves an embedded table's schema out when it matches the parent's, and the plan
    /// validator reads it that way. The CDC runtime does not: a missing schema means the provider's
    /// default there. Writing the parent's schema in keeps a root outside the default schema from
    /// validating against one table and ingesting another.
    /// </summary>
    private static CdcSinkTableConfig WithInheritedSchemas(CdcSinkTableConfig table)
    {
        Inherit(table.EmbeddedTables, table.SourceTableSchema);
        return table;

        static void Inherit(List<CdcSinkEmbeddedTableConfig>? embedded, string? parentSchema)
        {
            foreach (var child in embedded ?? [])
            {
                if (string.IsNullOrWhiteSpace(child.SourceTableSchema))
                    child.SourceTableSchema = parentSchema;

                Inherit(child.EmbeddedTables, child.SourceTableSchema);
            }
        }
    }

    /// <summary>
    /// The tables the operator discovered that no mapping captures. Coverage counts roots and
    /// embedded tables at any depth; a link does not count, because it only references documents
    /// another mapping has to produce.
    /// </summary>
    public static string[] UnmappedTables(CdcSinkConfiguration configuration, CdcSinkSourceSchema discovered)
    {
        var defaultSchema = DefaultSchemaOf(discovered);

        var covered = configuration
            .CollectAllTablesFlat(defaultSchema)
            .Select(t => t.FullName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return (discovered.Tables ?? [])
            .Select(t => $"{(string.IsNullOrEmpty(t.SourceTableSchema) ? defaultSchema : t.SourceTableSchema)}.{t.SourceTableName}")
            .Where(name => covered.Contains(name) == false)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    /// <summary>
    /// Discovery stamps every table with the schema it came from, so the schema the operator
    /// actually selected is a better default than a per-provider guess.
    /// </summary>
    private static string DefaultSchemaOf(CdcSinkSourceSchema discovered) =>
        (discovered.Tables ?? [])
            .Select(t => t.SourceTableSchema)
            .Where(s => string.IsNullOrWhiteSpace(s) == false)
            .GroupBy(s => s, StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(g => g.Count())
            .Select(g => g.Key)
            .FirstOrDefault() ?? string.Empty;
}
