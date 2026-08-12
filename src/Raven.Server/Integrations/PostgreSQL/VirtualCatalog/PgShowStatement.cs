using System;
using System.Collections.Generic;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    internal static class PgShowStatement
    {
        private const string ShowAll = "all";

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

        private static PgTable BuildSingleSettingTable(string settingName, string value)
        {
            return new PgTable
            {
                Columns = new List<PgColumn> { new(settingName, columnIndex: 0, pgType: PgText.Default, formatCode: PgFormat.Text) },
                Data = new List<PgDataRow> { new(new ReadOnlyMemory<byte>?[] { PgText.Default.ToBytes(value, PgFormat.Text) }) },
                CommandTag = CommandTag,
            };
        }

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

        private const string CommandTag = "SHOW";
    }
}
