using System.Collections.Generic;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    internal sealed class InformationSchemaCharacterSetsTable : PgVirtualTable
    {
        private static readonly IReadOnlyList<PgVirtualColumn> ColumnSchema = new PgVirtualColumn[]
        {
            new("character_set_name", PgName.Default),
        };

        private static readonly object[][] Rows =
        {
            new object[] { "UTF8" },
        };

        public override string SchemaName => "information_schema";
        public override string TableName => "character_sets";
        public override IReadOnlyList<PgVirtualColumn> Columns => ColumnSchema;

        public override IEnumerable<object[]> EnumerateRows(VirtualQueryContext ctx) => Rows;
    }
}
