using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.CdcSink.Schema
{
    /// <summary>
    /// SQL Server CDC catalog lookups (against cdc.change_tables and cdc.captured_columns).
    /// Shared between the streaming path (capture-instance + column resolution at task
    /// startup) and the schema-discovery endpoint (deciding whether a table/column is
    /// actually CDC-tracked). One source of truth — the UI shows the same view the
    /// streaming code will see.
    /// </summary>
    internal static class SqlServerCdcCatalogQueries
    {
        /// <summary>
        /// Returns all capture instances configured for a single source table, ordered
        /// oldest-first. CDC supports up to two concurrent capture instances per table
        /// (for online schema-change handoff); callers drain the oldest first.
        /// </summary>
        public static async Task<List<string>> FetchCaptureInstancesAsync(
            DbConnection connection, string schema, string tableName, CancellationToken ct)
        {
            var instances = new List<string>();

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
                SELECT capture_instance
                FROM cdc.change_tables
                WHERE source_object_id = OBJECT_ID(@fullTableName)
                ORDER BY create_date ASC";

            // Bracket-quote each part so OBJECT_ID resolves names that need delimiters (reserved
            // words like [Order], or names with dots/spaces); ] is escaped as ]] per SQL Server rules.
            CdcSinkProcess.AddParameter(cmd, "@fullTableName", $"[{schema.Replace("]", "]]")}].[{tableName.Replace("]", "]]")}]");

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                instances.Add(reader.GetString(0));

            return instances;
        }

        /// <summary>
        /// Returns the names of the columns captured by a specific capture instance, in
        /// the ordinal order CDC will emit them on the change-table row.
        /// </summary>
        public static async Task<List<string>> FetchCapturedColumnsAsync(
            DbConnection connection, string captureInstance, CancellationToken ct)
        {
            var columns = new List<string>();

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
                SELECT cc.column_name
                FROM cdc.captured_columns cc
                JOIN cdc.change_tables ct ON cc.object_id = ct.object_id
                WHERE ct.capture_instance = @capture
                ORDER BY cc.column_ordinal";

            CdcSinkProcess.AddParameter(cmd, "@capture", captureInstance);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                columns.Add(reader.GetString(0));

            return columns;
        }
    }
}
