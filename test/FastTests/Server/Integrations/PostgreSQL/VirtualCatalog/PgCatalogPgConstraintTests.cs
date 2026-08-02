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
    public class PgCatalogPgConstraintTests : RavenTestBase
    {
        public PgCatalogPgConstraintTests(ITestOutputHelper output) : base(output)
        {
        }

        // All four verbatim from SQLAlchemy 1.4.54's dialects/postgresql/base.py, bind parameter substituted.

        // PGDialect.get_pk_constraint(), the second of its two statements.
        private const string PkConsSql = """
                SELECT conname
                   FROM  pg_catalog.pg_constraint r
                   WHERE r.conrelid = {0} AND r.contype = 'p'
                   ORDER BY 1
                """;

        // PGDialect.get_foreign_keys().
        private const string FkSql = """
                  SELECT r.conname,
                        pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
                        n.nspname as conschema
                  FROM  pg_catalog.pg_constraint r,
                        pg_namespace n,
                        pg_class c

                  WHERE r.conrelid = {0} AND
                        r.contype = 'f' AND
                        c.oid = confrelid AND
                        n.oid = c.relnamespace
                  ORDER BY 1
                """;

        // PGDialect.get_unique_constraints().
        private const string UniqueSql = """
                    SELECT
                        cons.conname as name,
                        cons.conkey as key,
                        a.attnum as col_num,
                        a.attname as col_name
                    FROM
                        pg_catalog.pg_constraint cons
                        join pg_attribute a
                          on cons.conrelid = a.attrelid AND
                            a.attnum = ANY(cons.conkey)
                    WHERE
                        cons.conrelid = {0} AND
                        cons.contype = 'u'
                """;

        // PGDialect.get_check_constraints().
        private const string CheckSql = """
                    SELECT
                        cons.conname as name,
                        pg_get_constraintdef(cons.oid) as src
                    FROM
                        pg_catalog.pg_constraint cons
                    WHERE
                        cons.conrelid = {0} AND
                        cons.contype = 'c'
                """;

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_pk_constraint_names_the_constraint_by_a_statement_that_resolves_to_nothing()
        {
            using var store = GetDocumentStore();
            var (ctx, oid) = await PopulatedCatalog(store);

            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(PkConsSql, oid), ctx, out var table));

            Assert.Equal(new[] { "conname" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_foreign_keys_statement_resolves_and_reports_no_foreign_keys()
        {
            using var store = GetDocumentStore();
            var (ctx, oid) = await PopulatedCatalog(store);

            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(FkSql, oid), ctx, out var table));

            Assert.Equal(new[] { "conname", "condef", "conschema" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_unique_constraints_statement_resolves_and_reports_none()
        {
            using var store = GetDocumentStore();
            var (ctx, oid) = await PopulatedCatalog(store);

            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(UniqueSql, oid), ctx, out var table));

            Assert.Equal(new[] { "name", "key", "col_num", "col_name" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_check_constraints_statement_resolves_and_reports_none()
        {
            using var store = GetDocumentStore();
            var (ctx, oid) = await PopulatedCatalog(store);

            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(CheckSql, oid), ctx, out var table));

            Assert.Equal(new[] { "name", "src" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pg_constraint_is_registered_and_holds_no_rows()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select oid, conname, conrelid, contype, conkey, confrelid, conindid from pg_catalog.pg_constraint",
                new VirtualQueryContext(), out var table));

            Assert.Equal(
                new[] { "oid", "conname", "conrelid", "contype", "conkey", "confrelid", "conindid" },
                ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_get_constraintdef_is_not_implemented_and_is_never_reached()
        {
            using var store = GetDocumentStore();
            var (ctx, _) = await PopulatedCatalog(store);

            Assert.True(PgVirtualInterpreter.TryExecute(
                "select pg_catalog.pg_get_constraintdef(r.oid, true) from pg_catalog.pg_constraint r",
                ctx, out var overEmptyTable));
            Assert.Empty(overEmptyTable.Data);

            Assert.False(PgVirtualInterpreter.TryExecute(
                "select pg_catalog.pg_get_constraintdef(1, true)", ctx, out _));
        }

        private async Task<(VirtualQueryContext Ctx, string Oid)> PopulatedCatalog(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Order { Company = "companies/1", Freight = 42 }, "orders/1");
                session.SaveChanges();
            }

            var ctx = new VirtualQueryContext
            {
                Database = await Databases.GetDocumentDatabaseInstanceFor(store),
                Username = "root"
            };
            return (ctx, OidOf(ctx, "Orders"));
        }

        // The oid SQLAlchemy's get_table_oid() resolves before it runs any of the four.
        private static string OidOf(VirtualQueryContext ctx, string collection)
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                $"select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace " +
                $"where n.nspname = 'public' and c.relname = '{collection}' and c.relkind in ('r','v','m','f','p')",
                ctx, out var table));
            Assert.Single(table.Data);

            var cell = table.Data[0].ColumnData.Span[0];
            Assert.True(cell.HasValue);
            return Encoding.UTF8.GetString(cell.Value.Span);
        }

        private static IEnumerable<string> ColumnNames(PgTable table)
        {
            var names = new List<string>(table.Columns.Count);
            foreach (var column in table.Columns)
                names.Add(column.Name);
            return names;
        }

        private sealed class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public long Freight { get; set; }
        }
    }
}

