using System;
using System.Collections.Generic;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    /// <summary>
    /// Handles <c>SHOW &lt;setting&gt;</c> and <c>SHOW ALL</c>.
    /// <para>
    /// Unlike the other session-housekeeping statements (BEGIN / SET / DISCARD ALL - see
    /// <see cref="ProtocolCommandQuery"/>) SHOW is not a no-op: it returns a rowset the client
    /// reads. SQLAlchemy's <c>PGDialect.get_isolation_level()</c> issues
    /// <c>show transaction isolation level</c> on every connect and parses the value, so it has to
    /// live in the layer that can build a <see cref="PgTable"/> - hence here, next to the settings
    /// table it reads, rather than in the tag-only protocol-command path.
    /// </para>
    /// Values come from <see cref="PgSettings"/>, shared with <c>current_setting()</c>.
    /// </summary>
    internal static class PgShowStatement
    {
        private const string ShowAll = "all";

        // PG normalises the setting name during parsing: `SHOW TRANSACTION ISOLATION LEVEL`,
        // `show transaction isolation level` and `show transaction_isolation` all arrive as the
        // VariableShowStmt.Name "transaction_isolation". Case handling is therefore the parser's
        // job, not ours.
        public static bool TryExecute(string queryText, out PgTable result)
        {
            result = null;

            if (TryParse(queryText, out var settingName) == false)
                return false;

            if (string.Equals(settingName, ShowAll, StringComparison.OrdinalIgnoreCase))
            {
                result = BuildAllSettingsTable();
                return true;
            }

            if (PgSettings.TryGetValue(settingName, out var value) == false)
            {
                // Match PG: an unknown GUC is an ordinary client error (ERROR 42704), not an empty
                // rowset and not NULL. Returning either of those would tell the client the setting
                // exists but has no value, and it would carry that wrong conclusion forward.
                throw new PgErrorException(
                    PgErrorCodes.UndefinedObject,
                    $"unrecognized configuration parameter \"{settingName}\"");
            }

            result = BuildSingleSettingTable(settingName, value);
            return true;
        }

        private static bool TryParse(string queryText, out string settingName)
        {
            settingName = null;

            if (string.IsNullOrWhiteSpace(queryText))
                return false;

            var parseResult = SqlAstCache.GetOrParse(queryText);
            if (parseResult.IsSuccess == false || parseResult.Value?.Stmts is not { Count: 1 } stmts)
                return false;

            var show = stmts[0]?.Stmt?.VariableShowStmt;
            if (show == null)
                return false;

            settingName = show.Name;
            return string.IsNullOrEmpty(settingName) == false;
        }

        // PG's shape for `SHOW x`: exactly one row, one text column named after the setting.
        private static PgTable BuildSingleSettingTable(string settingName, string value)
        {
            return new PgTable
            {
                Columns = new List<PgColumn> { new(settingName, columnIndex: 0, pgType: PgText.Default, formatCode: PgFormat.Text) },
                Data = new List<PgDataRow> { new(new ReadOnlyMemory<byte>?[] { PgText.Default.ToBytes(value, PgFormat.Text) }) },
                CommandTag = CommandTag,
            };
        }

        // PG's shape for `SHOW ALL`: one row per setting with (name, setting, description).
        // We have no descriptions to give, so that column is NULL for every row - an invented
        // description would be a fabricated fact about the server. Only the settings PgSettings
        // knows are listed; this is a subset of a real PG's, the same way our pg_catalog tables are.
        private static PgTable BuildAllSettingsTable()
        {
            var table = new PgTable
            {
                Columns = new List<PgColumn>
                {
                    new("name", columnIndex: 0, pgType: PgText.Default, formatCode: PgFormat.Text),
                    new("setting", columnIndex: 1, pgType: PgText.Default, formatCode: PgFormat.Text),
                    new("description", columnIndex: 2, pgType: PgText.Default, formatCode: PgFormat.Text),
                },
                CommandTag = CommandTag,
            };

            foreach (var (name, value) in PgSettings.All)
            {
                table.Data.Add(new PgDataRow(new ReadOnlyMemory<byte>?[]
                {
                    PgText.Default.ToBytes(name, PgFormat.Text),
                    PgText.Default.ToBytes(value, PgFormat.Text),
                    null,
                }));
            }

            return table;
        }

        // PG reports the tag "SHOW" for these, not "SELECT <n>". Drivers that assert on the
        // CommandComplete tag (see the note in ProtocolCommandQuery) would trip on the latter.
        private const string CommandTag = "SHOW";
    }
}
