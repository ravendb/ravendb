using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.VirtualCatalog;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL.VirtualCatalog
{
    // pg_class is where SQLAlchemy - and therefore Apache Superset - looks for the table list:
    // PGDialect.get_table_names() joins pg_class to pg_namespace and filters on relkind, it never
    // reads information_schema.tables. While pg_class was empty, Superset connected but offered no
    // tables to build a dataset from (Zoho Desk #7031).
    //
    // Unlike PgVirtualInterpreterTests (which runs against a null-database context), these need a
    // live database: the rows come from its collections.
    public class PgCatalogPgClassTests : RavenTestBase
    {
        public PgCatalogPgClassTests(ITestOutputHelper output) : base(output)
        {
        }

        private const int PublicNamespaceOid = 2200;

        // The shape SQLAlchemy's PGDialect.get_table_names() sends.
        private const string SqlAlchemyGetTableNames =
            "select c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace " +
            "where n.nspname = 'public' and c.relkind in ('r','p')";

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_class_reports_one_ordinary_table_row_per_collection()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select oid, relname, relkind, relnamespace from pg_class", ctx, out var table));

            Assert.Equal(new[] { "oid", "relname", "relkind", "relnamespace" }, table.Columns.Select(c => c.Name));
            Assert.Equal(3, table.Data.Count);

            var byName = new Dictionary<string, (string Oid, string RelKind, string RelNamespace)>(StringComparer.Ordinal);
            for (int row = 0; row < table.Data.Count; row++)
            {
                byName[DecodeCell(table, row, column: 1)] =
                    (DecodeCell(table, row, column: 0), DecodeCell(table, row, column: 2), DecodeCell(table, row, column: 3));
            }

            Assert.Equal(new[] { "Companies", "Employees", "Orders" }, byName.Keys.OrderBy(n => n, StringComparer.Ordinal));

            foreach (var (name, values) in byName)
            {
                Assert.Equal("r", values.RelKind);
                Assert.Equal(PublicNamespaceOid.ToString(), values.RelNamespace);
                Assert.True(int.TryParse(values.Oid, out var oid) && oid > 0, $"'{name}' has a non-oid value '{values.Oid}'");
            }

            // oids identify a relation - two collections must never share one.
            Assert.Equal(byName.Count, byName.Values.Select(v => v.Oid).Distinct().Count());
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task SqlAlchemy_get_table_names_shape_lists_the_collections()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(SqlAlchemyGetTableNames, ctx, out var table));

            Assert.Single(table.Columns);
            Assert.Equal("relname", table.Columns[0].Name);
            Assert.Equal(new[] { "Companies", "Employees", "Orders" }, ColumnValues(table, column: 0).OrderBy(n => n, StringComparer.Ordinal));
        }

        // The invariant that broke: the two catalogs a client may reflect through must agree.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_class_and_information_schema_tables_report_the_same_names()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);

            Assert.True(PgVirtualInterpreter.TryExecute(
                "select relname from pg_class where relkind = 'r'", ctx, out var fromPgClass));
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select table_name from information_schema.tables where table_schema = 'public'", ctx, out var fromInformationSchema));

            Assert.Equal(
                ColumnValues(fromInformationSchema, column: 0).OrderBy(n => n, StringComparer.Ordinal),
                ColumnValues(fromPgClass, column: 0).OrderBy(n => n, StringComparer.Ordinal));
        }

        // Collection names keep their exact casing on both sides - 'Orders', not 'orders'.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_class_keeps_the_collection_name_casing()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select relname from pg_class where relname = 'Orders'", ctx, out var table));

            Assert.Single(table.Data);
            Assert.Equal("Orders", DecodeCell(table, row: 0, column: 0));
        }

        // SQLAlchemy's PGDialect.get_table_oid(), verbatim off the wire (1.4.54 + psycopg2). It runs
        // BEFORE get_columns() and its result keys every later reflection query, so while
        // pg_table_is_visible() was unimplemented this was rejected and Superset could not create a
        // dataset - even though pg_class itself already listed the table.
        //
        // This is the per-row call path: the predicate is evaluated once per pg_class row with a
        // column argument, not as a bare constant call (that form is in PgVirtualInterpreterTests).
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task SqlAlchemy_get_table_oid_resolves_a_collection_to_its_oid()
        {
            const string sql = """
                SELECT c.oid
                FROM pg_catalog.pg_class c
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE (pg_catalog.pg_table_is_visible(c.oid))
                AND c.relname = 'Orders' AND c.relkind in ('r', 'v', 'm', 'f', 'p')
                """;

            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(sql, ctx, out var table));

            Assert.Single(table.Columns);
            Assert.Equal("oid", table.Columns[0].Name);
            Assert.Single(table.Data);

            // The oid must be the same one pg_class reports for Orders - reflection keyed off a
            // different value would find no columns.
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select oid from pg_class where relname = 'Orders'", ctx, out var expected));
            Assert.Equal(DecodeCell(expected, row: 0, column: 0), DecodeCell(table, row: 0, column: 0));
        }

        // pgAdmin's schema-tree probe (the shape PgVirtualInterpreterTests covers against a null
        // database) reads pg_class.relnamespace inside correlated EXISTS subqueries. While pg_class
        // was empty those never evaluated per row; now they do, and an unresolvable column there
        // would reject the whole query instead of returning the namespace list.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pgadmin_schema_tree_query_is_still_accepted_over_a_populated_pg_class()
        {
            const string sql = """
                SELECT
                nsp.oid,
                nsp.nspname as name,
                pg_catalog.has_schema_privilege(nsp.oid, 'CREATE') as can_create,
                pg_catalog.has_schema_privilege(nsp.oid, 'USAGE') as has_usage,
                des.description
                FROM
                pg_catalog.pg_namespace nsp
                LEFT OUTER JOIN pg_catalog.pg_description des ON
                (des.objoid=nsp.oid AND des.classoid='pg_namespace'::regclass)
                WHERE
                nspname NOT LIKE E'pg\\_%' AND
                NOT (
                (nsp.nspname = 'pg_catalog' AND EXISTS
                (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pg_class' AND
                relnamespace = nsp.oid LIMIT 1)) OR
                (nsp.nspname = 'pgagent' AND EXISTS
                (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pga_job' AND
                relnamespace = nsp.oid LIMIT 1)) OR
                (nsp.nspname = 'information_schema' AND EXISTS
                (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'tables' AND
                relnamespace = nsp.oid LIMIT 1))
                )
                ORDER BY nspname
                """;

            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(sql, ctx, out var table));

            Assert.Equal(5, table.Columns.Count);
            // No collection is named 'pg_class' / 'tables', so both EXISTS arms stay false and the
            // namespaces survive the NOT(...) - the same list the null-database run produces.
            Assert.Equal(new[] { "information_schema", "public" }, ColumnValues(table, column: 1));
        }

        // No database on the context (the PgVirtualInterpreterTests case): still a well-formed,
        // empty rowset - never a rejected query.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pg_class_with_null_db_returns_empty_rowset()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select oid, relname, relkind, relnamespace from pg_class", new VirtualQueryContext(), out var table));

            Assert.Equal(4, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        private static void StoreSampleDocuments(IDocumentStore store)
        {
            using var session = store.OpenSession();
            session.Store(new Order(), "orders/1");
            session.Store(new Company(), "companies/1");
            session.Store(new Employee(), "employees/1");
            session.SaveChanges();
        }

        private async Task<VirtualQueryContext> CtxFor(IDocumentStore store)
            => new() { Database = await Databases.GetDocumentDatabaseInstanceFor(store), Username = "root" };

        private static IEnumerable<string> ColumnValues(PgTable table, int column)
        {
            for (int row = 0; row < table.Data.Count; row++)
                yield return DecodeCell(table, row, column);
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
        }

        private sealed class Company
        {
            public string Id { get; set; }
        }

        private sealed class Employee
        {
            public string Id { get; set; }
        }
    }
}
