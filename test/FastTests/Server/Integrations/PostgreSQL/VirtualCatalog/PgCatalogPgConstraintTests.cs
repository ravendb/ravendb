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
    // pg_constraint is the single table behind four of SQLAlchemy's reflection methods -
    // get_pk_constraint's second statement, get_foreign_keys, get_unique_constraints and
    // get_check_constraints - plus one join arm of get_indexes. Unregistered, each of those
    // rejected its whole statement.
    //
    // RavenDB has no constraints: no primary keys, no foreign keys (cross-document links are
    // document ids resolved with `load`, not FKs), no unique or check constraints. Empty is the
    // correct answer for all four, and none of them fabricate anything from a zero-row result -
    // they return an empty column list, an empty fkey list, and empty lists respectively.
    public class PgCatalogPgConstraintTests : RavenTestBase
    {
        public PgCatalogPgConstraintTests(ITestOutputHelper output) : base(output)
        {
        }

        // All four verbatim from SQLAlchemy 1.4.54's dialects/postgresql/base.py, with the bind
        // parameter substituted. None of them has a version-dependent branch.

        // PGDialect.get_pk_constraint(), the second of its two statements.
        private const string PkConsSql = """
                SELECT conname
                   FROM  pg_catalog.pg_constraint r
                   WHERE r.conrelid = {0} AND r.contype = 'p'
                   ORDER BY 1
                """;

        // PGDialect.get_foreign_keys(). Note the comma-separated FROM: pg_namespace and pg_class
        // are both populated here, so it is pg_constraint's emptiness alone that empties the
        // product.
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

        // get_foreign_keys and get_check_constraints both render the constraint through
        // pg_get_constraintdef(), and SQLAlchemy then regex-parses what comes back. We don't
        // implement it - there is no constraint to render - and with no rows it is never called.
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

