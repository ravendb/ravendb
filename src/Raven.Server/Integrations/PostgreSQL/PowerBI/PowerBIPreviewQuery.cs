using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public class PowerBIPreviewQuery : PowerBIRqlQuery
    {
        private readonly List<PgDataRow> _results;

        /// <summary>
        /// Matches PowerBI's query that's responsible for retreiving information about a table it wants to preview.
        /// Sent when selecting a table in PowerBI's GUI
        /// </summary>
        private static readonly Regex SqlRegex = new Regex(@"(?is)^\s*select\s+.*\s+from\s+INFORMATION_SCHEMA.columns\s+where\s+TABLE_SCHEMA\s+=\s+'public'\s+and\s+TABLE_NAME\s*=\s*'(?<table_name>[^']+)'\s+order\s+by\s+TABLE_SCHEMA\s*,\s*TABLE_NAME\s*,\s*ORDINAL_POSITION\s*$",
            RegexOptions.Compiled);

        public PowerBIPreviewQuery(DocumentDatabase documentDatabase, string tableName)
            : base($"from '{tableName}'", Array.Empty<int>(), documentDatabase, limit: 1)
        {
            _results = new List<PgDataRow>();
        }

        public static bool TryParse(string queryText, DocumentDatabase documentDatabase, out PgQuery pgQuery)
        {
            var match = SqlRegex.Match(queryText);

            if (match.Success)
            {
                var tableName = match.Groups["table_name"].Value;

                pgQuery = new PowerBIPreviewQuery(documentDatabase, tableName);
                return true;
            }

            pgQuery = null;
            return false;
        }

        public override async Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token)
        {
            foreach (var dataRow in _results)
            {
                await writer.WriteAsync(builder.DataRow(dataRow.ColumnData.Span), token);
            }

            await writer.WriteAsync(builder.CommandComplete($"SELECT {_results.Count}"), token);
        }

        public override void Bind(ICollection<byte[]> parameters, short[] parameterFormatCodes, short[] resultColumnFormatCodes)
        {
            // Note: We don't call base.Bind(..) because we only support parameters for this custom RQL, not the original SQL

            // TODO: After integration (we need code from RavenDB-17075), instead of directly putting the `tableName` in the query string,
            // uncomment the following line to add it as a parameter instead:
            //Parameters.Add("$tableName", _tableName);
        }

        public override async Task<ICollection<PgColumn>> Init(bool allowMultipleStatements = false)
        {
            var schema = await base.Init(allowMultipleStatements);

            int i = 0;
            foreach (var column in schema)
            {
                var columnData = new ReadOnlyMemory<byte>?[]
                {
                    Encoding.UTF8.GetBytes(column.Name), // column_name
                    BitConverter.GetBytes(IPAddress.HostToNetworkOrder(i)), // ordinal_position
                    Encoding.UTF8.GetBytes("YES"), // is_nullable
                    Encoding.UTF8.GetBytes(""), // data_type - easier to leave empty for us
                };

                _results.Add(new PgDataRow(columnData));
                i++;
            }

            return new List<PgColumn>
            {
                new("column_name", 0, PgName.Default,PgFormat.Text),
                new("ordinal_position", 1, PgInt4.Default, PgFormat.Binary),
                new("is_nullable", 2, PgVarchar.Default, PgFormat.Text),
                new("data_type", 3, PgVarchar.Default, PgFormat.Text),
            };
        }
    }
}
