using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.VirtualCatalog;
using Tests.Infrastructure;
using Xunit;
using static Tests.Infrastructure.PostgreSqlHelper;

namespace FastTests.Server.Integrations.PostgreSQL.VirtualCatalog
{
    public sealed class PgCatalogTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const int PublicNamespaceOid = 2200;

        #region pg_class

        // The shape SQLAlchemy's PGDialect.get_table_names() sends.
        private const string SqlAlchemyGetTableNames =
            "select c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace " +
            "where n.nspname = 'public' and c.relkind in ('r','p')";

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_class_reports_one_ordinary_table_row_per_collection()
        {
            using var store = GetDocumentStore();
            StoreThreeCollections(store);

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

            Assert.Equal(byName.Count, byName.Values.Select(v => v.Oid).Distinct().Count());
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task SqlAlchemy_get_table_names_shape_lists_the_collections()
        {
            using var store = GetDocumentStore();
            StoreThreeCollections(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(SqlAlchemyGetTableNames, ctx, out var table));

            Assert.Single(table.Columns);
            Assert.Equal("relname", table.Columns[0].Name);
            Assert.Equal(new[] { "Companies", "Employees", "Orders" }, ColumnValues(table, column: 0).OrderBy(n => n, StringComparer.Ordinal));
        }

        // SQLAlchemy 1.4's PGDialect.get_view_names() / get_sequence_names(), with the :schema bind
        // substituted. RavenDB has neither views nor sequences and pg_class reports relkind 'r' for every
        // collection, so both must resolve and return no rows rather than failing the reflection call.
        private const string SqlAlchemyGetViewNames =
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm')";

        private const string SqlAlchemyGetSequenceNames =
            "SELECT relname FROM pg_class c join pg_namespace n on n.oid=c.relnamespace " +
            "where relkind='S' and n.nspname='public'";

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task SqlAlchemy_get_view_names_shape_reports_no_views()
        {
            using var store = GetDocumentStore();
            StoreThreeCollections(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(SqlAlchemyGetViewNames, ctx, out var table));

            Assert.Single(table.Columns);
            Assert.Equal("relname", table.Columns[0].Name);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task SqlAlchemy_get_sequence_names_shape_reports_no_sequences()
        {
            using var store = GetDocumentStore();
            StoreThreeCollections(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(SqlAlchemyGetSequenceNames, ctx, out var table));

            Assert.Single(table.Columns);
            Assert.Equal("relname", table.Columns[0].Name);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_class_and_information_schema_tables_report_the_same_names()
        {
            using var store = GetDocumentStore();
            StoreThreeCollections(store);

            var ctx = await CtxFor(store);

            Assert.True(PgVirtualInterpreter.TryExecute(
                "select relname from pg_class where relkind = 'r'", ctx, out var fromPgClass));
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select table_name from information_schema.tables where table_schema = 'public'", ctx, out var fromInformationSchema));

            Assert.Equal(
                ColumnValues(fromInformationSchema, column: 0).OrderBy(n => n, StringComparer.Ordinal),
                ColumnValues(fromPgClass, column: 0).OrderBy(n => n, StringComparer.Ordinal));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_class_keeps_the_collection_name_casing()
        {
            using var store = GetDocumentStore();
            StoreThreeCollections(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select relname from pg_class where relname = 'Orders'", ctx, out var table));

            Assert.Single(table.Data);
            Assert.Equal("Orders", DecodeCell(table, row: 0, column: 0));
        }

        // SQLAlchemy's PGDialect.get_table_oid(), verbatim off the wire (1.4.54 + psycopg2).
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
            StoreThreeCollections(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(sql, ctx, out var table));

            Assert.Single(table.Columns);
            Assert.Equal("oid", table.Columns[0].Name);
            Assert.Single(table.Data);

            Assert.True(PgVirtualInterpreter.TryExecute(
                "select oid from pg_class where relname = 'Orders'", ctx, out var expected));
            Assert.Equal(DecodeCell(expected, row: 0, column: 0), DecodeCell(table, row: 0, column: 0));
        }

        // pgAdmin's schema-tree probe, which reads pg_class.relnamespace in correlated EXISTS arms.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pgadmin_schema_tree_query_is_still_accepted_over_a_populated_pg_class()
        {
            using var store = GetDocumentStore();
            StoreThreeCollections(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(PgAdminSchemaTreeQuery, ctx, out var table));

            Assert.Equal(5, table.Columns.Count);
            Assert.Equal(new[] { "information_schema", "public" }, ColumnValues(table, column: 1));
        }

        #endregion

        #region pg_class index options

        // SQLAlchemy 1.4.54's PGDialect.get_indexes() IDX_SQL, verbatim, with :table_oid substituted.
        private const string IdxSql = """
                      SELECT
                          i.relname as relname,
                          ix.indisunique, ix.indexprs,
                          a.attname, a.attnum, c.conrelid, ix.indkey::varchar,
                          ix.indoption::varchar, i.reloptions, am.amname,
                          pg_get_expr(ix.indpred, ix.indrelid),
                          ix.indnkeyatts as indnkeyatts
                      FROM
                          pg_class t
                                join pg_index ix on t.oid = ix.indrelid
                                join pg_class i on i.oid = ix.indexrelid
                                left outer join
                                    pg_attribute a
                                    on t.oid = a.attrelid and a.attnum = ANY(ix.indkey)
                                left outer join
                                    pg_constraint c
                                    on (ix.indrelid = c.conrelid and
                                        ix.indexrelid = c.conindid and
                                        c.contype in ('p', 'u', 'x'))
                                left outer join
                                    pg_am am
                                    on i.relam = am.oid
                      WHERE
                          t.relkind IN ('r', 'v', 'f', 'm', 'p')
                          and t.oid = {0}
                          and ix.indisprimary = 'f'
                      ORDER BY
                          t.relname,
                          i.relname
                    """;

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_indexes_statement_resolves_and_reports_no_indexes()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(IdxSql, OidOf(ctx, "Orders")), ctx, out var table));

            // SQLAlchemy unpacks positionally; unaliased expressions we name "?column?" are read by position.
            Assert.Equal(
                new[]
                {
                    "relname", "indisunique", "indexprs", "attname", "attnum", "conrelid",
                    "?column?", "?column?", "reloptions", "amname", "?column?", "indnkeyatts"
                },
                ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_class_declares_reloptions_and_relam_as_null_on_every_row()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select relname, reloptions, relam from pg_catalog.pg_class", ctx, out var table));

            Assert.Equal(new[] { "relname", "reloptions", "relam" }, ColumnNames(table));
            Assert.NotEmpty(table.Data);

            for (int row = 0; row < table.Data.Count; row++)
            {
                Assert.True(table.Data[row].ColumnData.Span[0].HasValue);
                Assert.False(table.Data[row].ColumnData.Span[1].HasValue);
                Assert.False(table.Data[row].ColumnData.Span[2].HasValue);
            }
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task The_existing_pg_class_columns_are_unchanged()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select oid, relname, relnamespace, relkind, typrelid from pg_catalog.pg_class " +
                $"where oid = {OidOf(ctx, "Orders")}", ctx, out var table));

            Assert.Equal(new[] { "oid", "relname", "relnamespace", "relkind", "typrelid" }, ColumnNames(table));
            Assert.Single(table.Data);
            Assert.Equal("Orders", DecodeCell(table, row: 0, column: 1));
            Assert.Equal("2200", DecodeCell(table, row: 0, column: 2));
            Assert.Equal("r", DecodeCell(table, row: 0, column: 3));
        }

        #endregion

        #region pg_attribute

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

            Assert.Equal(OrdersColumns, ColumnValues(table, column: 0));

            Assert.Equal(
                Enumerable.Range(1, table.Data.Count).Select(n => n.ToString()),
                ColumnValues(table, column: 1));

            for (int row = 0; row < table.Data.Count; row++)
            {
                Assert.Equal("-1", DecodeCell(table, row, column: 2));
                Assert.Equal("f", DecodeCell(table, row, column: 3));
                Assert.Equal("f", DecodeCell(table, row, column: 4));
                Assert.Equal("f", DecodeCell(table, row, column: 5));
            }
        }

        // PG stores the empty string, not NULL, for the plain case, and SQLAlchemy tests `!= ''`.
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

            Assert.Equal(
                new[] { "Companies", "Orders" },
                ColumnValues(joined, column: 0).Distinct().OrderBy(n => n, StringComparer.Ordinal));
        }

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

        #endregion

        #region pg_attrdef

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
            Assert.Equal(OrdersColumns, ColumnValues(table, column: 0));

            for (int row = 0; row < table.Data.Count; row++)
                Assert.False(table.Data[row].ColumnData.Span[1].HasValue);
        }

        #endregion

        #region pg_am

        // The access-method arm of SQLAlchemy 1.4.54's IDX_SQL, verbatim.
        private const string AccessMethodJoin = """
                  SELECT
                      i.relname as relname, am.amname
                  FROM
                      pg_class i
                            left outer join
                                pg_am am
                                on i.relam = am.oid
                  WHERE
                      i.relkind IN ('r', 'v', 'f', 'm', 'p')
                  ORDER BY
                      i.relname
                """;

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task The_access_method_join_resolves_and_names_no_access_method()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(AccessMethodJoin, ctx, out var table));

            Assert.Equal(new[] { "relname", "amname" }, ColumnNames(table));

            Assert.NotEmpty(table.Data);
            for (int row = 0; row < table.Data.Count; row++)
            {
                Assert.True(table.Data[row].ColumnData.Span[0].HasValue);
                Assert.False(table.Data[row].ColumnData.Span[1].HasValue);
            }
        }

        #endregion

        #region pg_index

        // SQLAlchemy 1.4.54's PGDialect.get_pk_constraint() PK_SQL, verbatim, with :table_oid substituted.
        private const string PkSql = """
                        SELECT a.attname
                        FROM pg_attribute a JOIN (
                            SELECT unnest(ix.indkey) attnum,
                                   generate_subscripts(ix.indkey, 1) ord
                            FROM pg_index ix
                            WHERE ix.indrelid = {0} AND ix.indisprimary
                            ) k ON a.attnum=k.attnum
                        WHERE a.attrelid = {0}
                        ORDER BY k.ord
                    """;

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_pk_constraint_statement_resolves_and_reports_no_key_columns()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(PkSql, OidOf(ctx, "Orders")), ctx, out var table));

            Assert.Equal(new[] { "attname" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        // The subquery's only FROM source is always-empty, so its columns are derived without executing the body.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task The_set_returning_functions_in_the_subquery_are_not_implemented_and_are_never_reached()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            var ordersOid = OidOf(ctx, "Orders");

            Assert.True(PgVirtualInterpreter.TryExecute(
                $"""
                 SELECT k.attnum, k.ord
                 FROM (
                     SELECT unnest(ix.indkey) attnum,
                            generate_subscripts(ix.indkey, 1) ord
                     FROM pg_index ix
                     WHERE ix.indrelid = {ordersOid} AND ix.indisprimary
                     ) k
                 """, ctx, out var subquery));
            Assert.Equal(new[] { "attnum", "ord" }, ColumnNames(subquery));
            Assert.Empty(subquery.Data);

            Assert.True(PgVirtualInterpreter.TryExecute(
                $"select attname from pg_attribute where attrelid = {ordersOid}", ctx, out var attributes));
            Assert.NotEmpty(attributes.Data);

            Assert.False(PgVirtualInterpreter.TryExecute("select unnest('{1,2}')", ctx, out _));
            Assert.False(PgVirtualInterpreter.TryExecute("select generate_subscripts('{1,2}', 1)", ctx, out _));
        }

        #endregion

        #region pg_constraint

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
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(PkConsSql, OidOf(ctx, "Orders")), ctx, out var table));

            Assert.Equal(new[] { "conname" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_foreign_keys_statement_resolves_and_reports_no_foreign_keys()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(FkSql, OidOf(ctx, "Orders")), ctx, out var table));

            Assert.Equal(new[] { "conname", "condef", "conschema" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_unique_constraints_statement_resolves_and_reports_none()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(UniqueSql, OidOf(ctx, "Orders")), ctx, out var table));

            Assert.Equal(new[] { "name", "key", "col_num", "col_name" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_check_constraints_statement_resolves_and_reports_none()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(CheckSql, OidOf(ctx, "Orders")), ctx, out var table));

            Assert.Equal(new[] { "name", "src" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_get_constraintdef_is_not_implemented_and_is_never_reached()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);

            Assert.True(PgVirtualInterpreter.TryExecute(
                "select pg_catalog.pg_get_constraintdef(r.oid, true) from pg_catalog.pg_constraint r",
                ctx, out var overEmptyTable));
            Assert.Empty(overEmptyTable.Data);

            Assert.False(PgVirtualInterpreter.TryExecute(
                "select pg_catalog.pg_get_constraintdef(1, true)", ctx, out _));
        }

        #endregion

        #region pg_sequence

        // SQLAlchemy 1.4.54's PGDialect.get_columns() SQL_COLS, verbatim.
        private const string SqlCols = """
                        SELECT a.attname,
                          pg_catalog.format_type(a.atttypid, a.atttypmod),
                          (
                            SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                            FROM pg_catalog.pg_attrdef d
                            WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
                            AND a.atthasdef
                          ) AS DEFAULT,
                          a.attnotnull,
                          a.attrelid as table_oid,
                          pgd.description as comment,
                          a.attgenerated as generated,
                            (SELECT json_build_object(
                                'always', a.attidentity = 'a',
                                'start', s.seqstart,
                                'increment', s.seqincrement,
                                'minvalue', s.seqmin,
                                'maxvalue', s.seqmax,
                                'cache', s.seqcache,
                                'cycle', s.seqcycle)
                            FROM pg_catalog.pg_sequence s
                            JOIN pg_catalog.pg_class c on s.seqrelid = c."oid"
                            WHERE c.relkind = 'S'
                            AND a.attidentity != ''
                            AND s.seqrelid = pg_catalog.pg_get_serial_sequence(
                                a.attrelid::regclass::text, a.attname
                            )::regclass::oid
                            ) as identity_options
                        FROM pg_catalog.pg_attribute a
                        LEFT JOIN pg_catalog.pg_description pgd ON (
                            pgd.objoid = a.attrelid AND pgd.objsubid = a.attnum)
                        WHERE a.attrelid = {0}
                        AND a.attnum > 0 AND NOT a.attisdropped
                        ORDER BY a.attnum
                    """;

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_columns_statement_resolves_and_reports_the_collections_columns()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            var ordersOid = OidOf(ctx, "Orders");

            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(SqlCols, ordersOid), ctx, out var table));

            Assert.Equal(
                new[] { "attname", "?column?", "default", "attnotnull", "table_oid", "comment", "generated", "identity_options" },
                ColumnNames(table));
            Assert.Equal(OrdersColumns, ColumnValues(table, column: 0));
            Assert.Equal(OrdersColumnTypes, ColumnValues(table, column: 1));

            for (int row = 0; row < table.Data.Count; row++)
            {
                Assert.False(table.Data[row].ColumnData.Span[2].HasValue);
                Assert.Equal("f", DecodeCell(table, row, column: 3));
                Assert.Equal(ordersOid, DecodeCell(table, row, column: 4));
                Assert.False(table.Data[row].ColumnData.Span[5].HasValue);
                Assert.Equal(string.Empty, DecodeCell(table, row, column: 6));
                Assert.False(table.Data[row].ColumnData.Span[7].HasValue);
            }
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task The_identity_subquerys_machinery_is_not_implemented_and_is_never_reached()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);

            Assert.True(PgVirtualInterpreter.TryExecute(
                """
                SELECT json_build_object('start', s.seqstart, 'cycle', s.seqcycle)
                FROM pg_catalog.pg_sequence s
                JOIN pg_catalog.pg_class c on s.seqrelid = c."oid"
                WHERE c.relkind = 'S'
                AND s.seqrelid = pg_catalog.pg_get_serial_sequence('Orders'::regclass::text, 'id')::regclass::oid
                """, ctx, out var overEmptyPgSequence));
            Assert.Empty(overEmptyPgSequence.Data);

            Assert.True(PgVirtualInterpreter.TryExecute("select oid from pg_class where relkind = 'r'", ctx, out var pgClass));
            Assert.NotEmpty(pgClass.Data);

            Assert.False(PgVirtualInterpreter.TryExecute(
                "select json_build_object('always', true)", ctx, out _));
            Assert.False(PgVirtualInterpreter.TryExecute(
                "select pg_catalog.pg_get_serial_sequence('Orders', 'id')", ctx, out _));
        }

        #endregion

        private static readonly string[] OrdersColumns =
            ["id", "Company", "Freight", "Discount", "Shipped", "OrderedAt", "Processing", "Lines", "json"];

        private static readonly string[] OrdersColumnTypes =
            ["text", "text", "bigint", "double precision", "boolean", "timestamp with time zone", "interval", "json", "json"];

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

        private static void StoreThreeCollections(IDocumentStore store)
        {
            using var session = store.OpenSession();
            session.Store(new Order(), "orders/1");
            session.Store(new Company(), "companies/1");
            session.Store(new Employee(), "employees/1");
            session.SaveChanges();
        }

        private async Task<VirtualQueryContext> CtxFor(IDocumentStore store)
            => new() { Database = await Databases.GetDocumentDatabaseInstanceFor(store), Username = "root" };

        private static string OidOf(VirtualQueryContext ctx, string collection)
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                $"select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace " +
                $"where n.nspname = 'public' and c.relname = '{collection}' and c.relkind in ('r','v','m','f','p')",
                ctx, out var table));
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

        private sealed class Employee
        {
            public string Id { get; set; }
        }
    }
}
