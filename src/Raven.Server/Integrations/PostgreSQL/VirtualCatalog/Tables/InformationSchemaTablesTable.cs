using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    internal sealed class InformationSchemaTablesTable : PgVirtualTable
    {
        private const string PublicSchema = "public";
        private const string BaseTableType = "BASE TABLE";

        private static readonly IReadOnlyList<PgVirtualColumn> ColumnSchema = new PgVirtualColumn[]
        {
            new("table_schema", PgName.Default),
            new("table_name", PgName.Default),
            new("table_type", PgVarchar.Default),
        };

        public override string SchemaName => "information_schema";
        public override string TableName => "tables";
        public override IReadOnlyList<PgVirtualColumn> Columns => ColumnSchema;

        public override IEnumerable<object[]> EnumerateRows(VirtualQueryContext ctx)
        {
            if (ctx?.Database == null)
                yield break;

            var collectionNames = new List<string>();

            using (ctx.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var collection in ctx.Database.DocumentsStorage.GetCollections(context))
                {
                    if (CollectionName.IsHiLoCollection(collection.Name))
                        continue;

                    collectionNames.Add(collection.Name);
                }
            }

            foreach (var name in collectionNames)
            {
                yield return new object[] { PublicSchema, name, BaseTableType };
            }
        }
    }
}
