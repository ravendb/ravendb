using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public class PBIAllCollectionsQuery : RqlQuery
    {
        public PBIAllCollectionsQuery(string queryText, int[] parametersDataTypes, IDocumentStore documentStore)
            : base(queryText, parametersDataTypes, documentStore)
        {
        }

        public static bool TryParse(string queryText, int[] parametersDataTypes, IDocumentStore documentStore, out PgQuery pgQuery)
        {
            const string tableQuery = "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\nfrom INFORMATION_SCHEMA.tables\nwhere TABLE_SCHEMA not in ('information_schema', 'pg_catalog')\norder by TABLE_SCHEMA, TABLE_NAME";

            queryText = queryText.Replace("\r\n", "\n").Replace("\r", "\n");
            if (queryText.Equals(tableQuery, StringComparison.OrdinalIgnoreCase))
            {
                pgQuery = new PBIAllCollectionsQuery(queryText, parametersDataTypes, documentStore);
                return true;
            }

            pgQuery = null;
            return false;
        }

        public override async Task<ICollection<PgColumn>> Init(bool allowMultipleStatements = false)
        {
            return new PgColumn[]
            {
                new("table_schema", 0, PgName.Default, PgFormat.Text),
                new("table_name", 1, PgName.Default, PgFormat.Text),
                new("table_type", 2, PgVarchar.Default, PgFormat.Text),
            };
        }

        public override async Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token)
        {
            var collectionStats = DocumentStore.Maintenance.Send(new GetCollectionStatisticsOperation());

            foreach (var collection in collectionStats.Collections.Keys)
            {
                var dataRow = new ReadOnlyMemory<byte>?[]
                {
                        Encoding.UTF8.GetBytes("public"),
                        Encoding.UTF8.GetBytes(collection),
                        Encoding.UTF8.GetBytes("BASE TABLE"),
                };

                await writer.WriteAsync(builder.DataRow(dataRow), token);
            }

            await writer.WriteAsync(builder.CommandComplete($"SELECT {collectionStats.Collections.Count}"), token);
        }
    }
}
