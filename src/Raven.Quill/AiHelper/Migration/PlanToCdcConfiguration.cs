using System.Text.Json;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Endpoints;

namespace Raven.Quill.AiHelper.Migration;

public static class PlanToCdcConfiguration
{
    public static CdcSinkConfiguration Build(IEnumerable<PlanEntry> entries, CdcSinkConfiguration? previousMapping)
    {
        var configuration = new CdcSinkConfiguration
        {
            Name = string.IsNullOrWhiteSpace(previousMapping?.Name) ? WizardEndpoints.DefaultCdcTaskName : previousMapping.Name,
            ConnectionStringName = string.IsNullOrWhiteSpace(previousMapping?.ConnectionStringName)
                ? WizardEndpoints.SourceConnectionStringName
                : previousMapping.ConnectionStringName
        };

        foreach (var entry in entries)
        {
            if (entry.Config is not null)
                configuration.Tables.Add(WithInheritedSchemas(entry.Config));
        }

        return configuration;
    }

    private static CdcSinkTableConfig WithInheritedSchemas(CdcSinkTableConfig registered)
    {
        var table = JsonSerializer.Deserialize<CdcSinkTableConfig>(JsonSerializer.SerializeToUtf8Bytes(registered))!;
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

    public static string[] UnmappedTables(CdcSinkConfiguration configuration, CdcSinkSourceSchema discovered, string defaultSchema)
    {
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

    public static void ValidateJoinColumnsAgainstSchema(CdcSinkConfiguration configuration, CdcSinkSourceSchema schema, List<string> errors)
    {
        foreach (var table in configuration.Tables)
            ValidateScope(table.SourceTableSchema, table.SourceTableName, table.CollectionName, table.EmbeddedTables, table.LinkedTables);

        void ValidateScope(
            string? tableSchema,
            string tableName,
            string description,
            List<CdcSinkEmbeddedTableConfig>? embeddedTables,
            List<CdcSinkLinkedTableConfig>? linkedTables)
        {
            var columns = SourceColumnsOf(tableSchema, tableName);

            foreach (var linked in linkedTables ?? [])
                CheckJoinColumns(linked.JoinColumns, columns, $"Linked table '{linked.SourceTableName}' under '{description}'");

            foreach (var embedded in embeddedTables ?? [])
            {
                ValidateScope(embedded.SourceTableSchema, embedded.SourceTableName,
                    $"{description}.{embedded.PropertyName}", embedded.EmbeddedTables, embedded.LinkedTables);

                CheckJoinColumns(embedded.JoinColumns, SourceColumnsOf(embedded.SourceTableSchema, embedded.SourceTableName),
                    $"Embedded table '{embedded.SourceTableName}' under '{description}'");
            }
        }

        HashSet<string>? SourceColumnsOf(string? tableSchema, string tableName) => schema.Tables
            .FirstOrDefault(table =>
                string.Equals(table.SourceTableName, tableName, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(table.SourceTableSchema ?? string.Empty, tableSchema ?? string.Empty, StringComparison.OrdinalIgnoreCase))
            ?.Columns.Select(column => column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

        void CheckJoinColumns(List<string>? joinColumns, HashSet<string>? sourceColumns, string description)
        {
            if (sourceColumns is null)
                return;

            foreach (var joinColumn in joinColumns ?? [])
            {
                if (string.IsNullOrWhiteSpace(joinColumn) || sourceColumns.Contains(joinColumn))
                    continue;

                errors.Add($"{description}: join column '{joinColumn}' is not a column of the source table. " +
                    $"Its columns are: {string.Join(", ", sourceColumns)}.");
            }
        }
    }
}
