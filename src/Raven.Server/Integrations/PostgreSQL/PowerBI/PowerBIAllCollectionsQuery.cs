using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public class PowerBIAllCollectionsQuery : PowerBIRqlQuery
    {
        private static readonly string TableQuery = "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\nfrom INFORMATION_SCHEMA.tables\nwhere TABLE_SCHEMA not in ('information_schema', 'pg_catalog')\norder by TABLE_SCHEMA, TABLE_NAME".NormalizeLineEndings();

        public PowerBIAllCollectionsQuery(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase)
            : base(queryText, parametersDataTypes, documentDatabase)
        {
        }

        public static bool TryParse(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out PgQuery pgQuery)
        {
            queryText = queryText.NormalizeLineEndings();

            if (queryText.Equals(TableQuery, StringComparison.OrdinalIgnoreCase))
            {
                pgQuery = new PowerBIAllCollectionsQuery(queryText, parametersDataTypes, documentDatabase);
                return true;
            }

            pgQuery = null;
            return false;
        }

        public override Task<ICollection<PgColumn>> Init(bool allowMultipleStatements = false)
        {
            return Task.FromResult<ICollection<PgColumn>>(new PgColumn[]
            {
                new("table_schema", 0, PgName.Default, PgFormat.Text),
                new("table_name", 1, PgName.Default, PgFormat.Text),
                new("table_type", 2, PgVarchar.Default, PgFormat.Text),
            });
        }

        public override async Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token)
        {
            var collections = new List<string>();

            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var collection in DocumentDatabase.DocumentsStorage.GetCollections(context))
                {
                    if (CollectionName.IsHiLoCollection(collection.Name))
                        continue;

                    collections.Add(collection.Name);
                }
            }

            foreach (var collection in collections)
            {
                var dataRow = new ReadOnlyMemory<byte>?[]
                {
                    Encoding.UTF8.GetBytes("public"),
                    Encoding.UTF8.GetBytes(collection),
                    Encoding.UTF8.GetBytes("BASE TABLE"),
                };

                await writer.WriteAsync(builder.DataRow(dataRow), token);
            }

            await writer.WriteAsync(builder.CommandComplete($"SELECT {collections.Count}"), token);
        }
    }
}
