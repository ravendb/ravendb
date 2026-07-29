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
    // pg_attribute is where SQLAlchemy - and therefore Apache Superset - reads a table's columns:
    // PGDialect.get_columns() selects from pg_attribute keyed by attrelid, it never reads
    // information_schema.columns. While pg_attribute was empty, Superset listed the tables (those
    // come from pg_class) but every one of them reflected with zero columns, so creating a dataset
    // failed with "Unable to load columns for the selected table" (Zoho Desk #7031).
    //
    // Like PgCatalogPgClassTests these need a live database - the rows come from its collections.
    public class PgCatalogPgAttributeTests : RavenTestBase
    {
        public PgCatalogPgAttributeTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_attribute_reports_a_row_per_column_of_the_collection()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            var ordersOid = OidOf(ctx, "Orders");

            Assert.True(PgVirtualInterpreter.TryExecute(
                $"select attname, attnum, atttypmod, attnotnull, atthasdef, attisdropped from pg_attribute " +
                $"where attrelid = {ordersOid} order by attnum", ctx, out var table));

            // The synthetic id column, the document's fields in insertion order, then json - the
            // same shape RqlQuery emits in its RowDescription.
            Assert.Equal(
                new[] { "id", "Company", "Freight", "Discount", "Shipped", "OrderedAt", "Processing", "Lines", "json" },
                ColumnValues(table, column: 0));

            // attnum is 1-based and gapless; SQLAlchemy filters system attributes with `attnum > 0`.
            Assert.Equal(
                Enumerable.Range(1, table.Data.Count).Select(n => n.ToString()),
                ColumnValues(table, column: 1));

            for (int row = 0; row < table.Data.Count; row++)
            {
                Assert.Equal("-1", DecodeCell(table, row, column: 2));  // atttypmod - no length modifier
                Assert.Equal("f", DecodeCell(table, row, column: 3));   // attnotnull - a document may omit a field
                Assert.Equal("f", DecodeCell(table, row, column: 4));   // atthasdef - RavenDB has no column defaults
                Assert.Equal("f", DecodeCell(table, row, column: 5));   // attisdropped
            }
        }

        // PG stores the empty string - not NULL - for a column that is neither an identity nor a
        // generated column, and SQLAlchemy's get_columns() tests `a.attidentity != ''`. RavenDB has
        // no identity or generated columns at all, so every row must read as that plain case.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Every_column_reads_as_a_plain_non_identity_non_generated_column()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select attidentity, attgenerated from pg_attribute", ctx, out var table));

            Assert.NotEmpty(table.Data);
            for (int row = 0; row < table.Data.Count; row++)
            {
                Assert.Equal(string.Empty, DecodeCell(table, row, column: 0));
                Assert.Equal(string.Empty, DecodeCell(table, row, column: 1));
            }
        }

        // The invariant the whole feature rests on: attrelid has to be the oid pg_class derives. If
        // the two disagreed, every join between them would silently return nothing - a client would
        // see the table list and then zero columns, which is exactly the bug being fixed. So: no
        // pg_attribute row may be lost when inner-joined to pg_class on attrelid = oid.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Attrelid_matches_the_oid_pg_class_derives()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);

            Assert.True(PgVirtualInterpreter.TryExecute(
                "select attname from pg_attribute", ctx, out var unjoined));
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select c.relname, a.attname from pg_class c join pg_attribute a on a.attrelid = c.oid",
                ctx, out var joined));

            Assert.NotEmpty(unjoined.Data);
            Assert.Equal(unjoined.Data.Count, joined.Data.Count);

            // And every row landed on a real collection, not some unrelated relation.
            Assert.Equal(
                new[] { "Companies", "Orders" },
                ColumnValues(joined, column: 0).Distinct().OrderBy(n => n, StringComparer.Ordinal));
        }

        // The two catalogs a client may reflect through must report the same columns in the same
        // order with the same types. information_schema.columns spells the type out; pg_attribute
        // reports an oid that format_type(atttypid, atttypmod) renders - which is how SQLAlchemy
        // asks for it. Both come from CollectionCatalog, and this is what pins them together.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_attribute_and_information_schema_columns_report_the_same_columns()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            var ordersOid = OidOf(ctx, "Orders");

            Assert.True(PgVirtualInterpreter.TryExecute(
                $"select attname, format_type(atttypid, atttypmod) from pg_attribute " +
                $"where attrelid = {ordersOid} order by attnum", ctx, out var fromPgAttribute));
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select column_name, data_type from information_schema.columns " +
                "where table_name = 'Orders' order by ordinal_position", ctx, out var fromInformationSchema));

            Assert.NotEmpty(fromPgAttribute.Data);
            Assert.Equal(ColumnValues(fromInformationSchema, column: 0), ColumnValues(fromPgAttribute, column: 0));
            Assert.Equal(ColumnValues(fromInformationSchema, column: 1), ColumnValues(fromPgAttribute, column: 1));
        }

        // The document's field types, as SQLAlchemy reads them. Covers every arm of the mapping a
        // sample document can reach, including interval - which only becomes reachable through
        // format_type once pg_attribute has rows.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Atttypid_renders_the_type_of_each_document_field()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            var ordersOid = OidOf(ctx, "Orders");

            Assert.True(PgVirtualInterpreter.TryExecute(
                $"select attname, format_type(atttypid, atttypmod) from pg_attribute " +
                $"where attrelid = {ordersOid} order by attnum", ctx, out var table));

            var byName = new Dictionary<string, string>(StringComparer.Ordinal);
            for (int row = 0; row < table.Data.Count; row++)
                byName[DecodeCell(table, row, column: 0)] = DecodeCell(table, row, column: 1);

            Assert.Equal("text", byName["id"]);
            Assert.Equal("text", byName["Company"]);
            Assert.Equal("bigint", byName["Freight"]);
            Assert.Equal("double precision", byName["Discount"]);
            Assert.Equal("boolean", byName["Shipped"]);
            Assert.Equal("timestamp with time zone", byName["OrderedAt"]);
            Assert.Equal("interval", byName["Processing"]);
            Assert.Equal("json", byName["Lines"]);
            Assert.Equal("json", byName["json"]);
        }

        // Scoping by the attrelid predicate is an optimization - reflecting one relation shouldn't
        // read a document from every collection. It must not change the answer.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Scoping_by_attrelid_returns_what_filtering_every_relation_would()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            var companiesOid = OidOf(ctx, "Companies");

            Assert.True(PgVirtualInterpreter.TryExecute(
                $"select attname from pg_attribute where attrelid = {companiesOid} order by attnum", ctx, out var scoped));
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select a.attname from pg_class c join pg_attribute a on a.attrelid = c.oid " +
                "where c.relname = 'Companies' order by a.attnum", ctx, out var viaJoin));

            Assert.NotEmpty(scoped.Data);
            Assert.Equal(ColumnValues(viaJoin, column: 0), ColumnValues(scoped, column: 0));
        }

        // An empty collection has no sample document, so there is no column shape to report -
        // information_schema.columns has always behaved the same way. Still a well-formed rowset.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task A_collection_with_no_documents_reports_no_columns()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Company(), "companies/2");
                session.SaveChanges();
                session.Delete("companies/1");
                session.Delete("companies/2");
                session.SaveChanges();
            }

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select c.relname, a.attname from pg_class c join pg_attribute a on a.attrelid = c.oid",
                ctx, out var table));

            Assert.DoesNotContain("Companies", ColumnValues(table, column: 0));
            Assert.Contains("Orders", ColumnValues(table, column: 0));
        }

        // No database on the context (the PgVirtualInterpreterTests case): still a well-formed,
        // empty rowset - never a rejected query.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pg_attribute_with_null_db_returns_empty_rowset()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select attrelid, attname, atttypid, attnum from pg_attribute", new VirtualQueryContext(), out var table));

            Assert.Equal(4, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        private static void StoreSampleDocuments(IDocumentStore store)
        {
            using var session = store.OpenSession();
            session.Store(new Order
            {
                Company = "companies/1",
                Freight = 42,
                Discount = 0.15,
                Shipped = true,
                OrderedAt = new DateTime(2026, 7, 29, 10, 0, 0, DateTimeKind.Utc),
                Processing = TimeSpan.FromMinutes(30),
                Lines = new[] { "products/1", "products/2" }
            }, "orders/1");
            session.Store(new Company { Name = "Raven" }, "companies/1");
            session.SaveChanges();
        }

        private async Task<VirtualQueryContext> CtxFor(IDocumentStore store)
            => new() { Database = await Databases.GetDocumentDatabaseInstanceFor(store), Username = "root" };

        // The oid pg_class reports for a collection - the same lookup SQLAlchemy's get_table_oid()
        // does before it reads pg_attribute.
        private static string OidOf(VirtualQueryContext ctx, string collection)
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                $"select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace " +
                $"where n.nspname = 'public' and c.relname = '{collection}' and c.relkind in ('r','v','m','f','p')",
                ctx, out var table));
            Assert.Single(table.Data);
            return DecodeCell(table, row: 0, column: 0);
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
            public long Freight { get; set; }
            public double Discount { get; set; }
            public bool Shipped { get; set; }
            public DateTime OrderedAt { get; set; }
            public TimeSpan Processing { get; set; }
            public string[] Lines { get; set; }
        }

        private sealed class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
