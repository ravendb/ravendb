using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.VirtualCatalog;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL.VirtualCatalog
{
    public class PgCatalogPgAttrdefTests : RavenTestBase
    {
        public PgCatalogPgAttrdefTests(ITestOutputHelper output) : base(output)
        {
        }

        // The default-lookup fragment exactly as SQLAlchemy's SQL_COLS spells it.
        private const string DefaultLookupOverPgAttribute = """
            SELECT a.attname,
              (
                SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                FROM pg_catalog.pg_attrdef d
                WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
                AND a.atthasdef
              ) AS DEFAULT
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = {0}
            AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """;

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task The_correlated_pg_attrdef_subquery_resolves_and_reports_no_default()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            var sql = string.Format(DefaultLookupOverPgAttribute, OidOf(ctx, "Orders"));

            Assert.True(PgVirtualInterpreter.TryExecute(sql, ctx, out var table));

            Assert.Equal(new[] { "attname", "default" }, ColumnNames(table));
            Assert.Equal(new[] { "id", "Company", "json" }, ColumnValues(table, column: 0));

            for (int row = 0; row < table.Data.Count; row++)
                Assert.False(table.Data[row].ColumnData.Span[1].HasValue);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pg_attrdef_is_registered_and_holds_no_rows()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select oid, adrelid, adnum, adbin from pg_catalog.pg_attrdef", new VirtualQueryContext(), out var table));

            Assert.Equal(new[] { "oid", "adrelid", "adnum", "adbin" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pg_get_expr_is_not_implemented_and_is_never_reached_over_an_empty_pg_attrdef()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select pg_catalog.pg_get_expr(d.adbin, d.adrelid) from pg_catalog.pg_attrdef d",
                new VirtualQueryContext(), out var overEmptyTable));
            Assert.Empty(overEmptyTable.Data);

            Assert.False(PgVirtualInterpreter.TryExecute(
                "select pg_catalog.pg_get_expr(null, null)", new VirtualQueryContext(), out _));
        }

        private static void StoreSampleDocuments(IDocumentStore store)
        {
            using var session = store.OpenSession();
            session.Store(new Order { Company = "companies/1" }, "orders/1");
            session.SaveChanges();
        }

        private async Task<VirtualQueryContext> CtxFor(IDocumentStore store)
            => new() { Database = await Databases.GetDocumentDatabaseInstanceFor(store), Username = "root" };

        private static string OidOf(VirtualQueryContext ctx, string collection)
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                $"select oid from pg_class where relname = '{collection}'", ctx, out var table));
            Assert.Single(table.Data);
            return DecodeCell(table, row: 0, column: 0);
        }

        private static IEnumerable<string> ColumnNames(PgTable table)
        {
            var names = new List<string>(table.Columns.Count);
            foreach (var column in table.Columns)
                names.Add(column.Name);
            return names;
        }

        private static IEnumerable<string> ColumnValues(PgTable table, int column)
        {
            var values = new List<string>(table.Data.Count);
            for (int row = 0; row < table.Data.Count; row++)
                values.Add(DecodeCell(table, row, column));
            return values;
        }

        private static string DecodeCell(PgTable table, int row, int column)
        {
            var cell = table.Data[row].ColumnData.Span[column];
            Assert.True(cell.HasValue);
            return Encoding.UTF8.GetString(cell.Value.Span);
        }

        private sealed class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
        }
    }
}
