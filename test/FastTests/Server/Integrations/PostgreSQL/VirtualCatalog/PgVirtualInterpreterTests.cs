using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Raven.Server.Integrations.PostgreSQL.VirtualCatalog;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL.VirtualCatalog
{
    public sealed class PgVirtualInterpreterTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Version_function_returns_single_row()
        {
            Assert.True(PgVirtualInterpreter.TryExecute("select version()", EmptyCtx(), out var table));
            Assert.NotNull(table);
            Assert.Single(table.Columns);
            Assert.Equal("version", table.Columns[0].Name);
            Assert.Single(table.Data);
            Assert.StartsWith("PostgreSQL", DecodeCell(table, row: 0, column: 0));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Current_setting_max_index_keys_returns_32()
        {
            Assert.True(PgVirtualInterpreter.TryExecute("select current_setting('max_index_keys')", EmptyCtx(), out var table));
            Assert.NotNull(table);
            Assert.Single(table.Data);
            Assert.Equal("32", DecodeCell(table, row: 0, column: 0));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Current_setting_unknown_setting_falls_through()
        {
            Assert.False(PgVirtualInterpreter.TryExecute("select current_setting('not_a_real_setting')", EmptyCtx(), out var table));
            Assert.Null(table);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Version_function_with_alias_uses_alias_as_column_name()
        {
            Assert.True(PgVirtualInterpreter.TryExecute("select version() as v", EmptyCtx(), out var table));
            Assert.NotNull(table);
            Assert.Single(table.Columns);
            Assert.Equal("v", table.Columns[0].Name);
        }

        // format_type(oid) maps a builtin type oid to its SQL-standard display name (23 -> integer).
        // The parameterized pgAdmin probe passes $1, which is NULL at parse time and yields a NULL
        // row (that path is unchanged); a concrete oid like 23 resolves to the type name.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Format_type_scalar_resolves_oid_to_type_name()
        {
            Assert.True(PgVirtualInterpreter.TryExecute("select format_type(23)", EmptyCtx(), out var table));
            Assert.NotNull(table);
            Assert.Single(table.Columns);
            Assert.Single(table.Data);
            Assert.Equal("integer", DecodeCell(table, row: 0, column: 0));
        }

        // A non-oid arg must fall through, not echo a bogus type name back as if it resolved.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Format_type_unresolvable_oid_falls_through()
        {
            Assert.False(PgVirtualInterpreter.TryExecute("select format_type('not_an_oid')", EmptyCtx(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Star_projection_over_character_sets_returns_full_table()
        {
            Assert.True(PgVirtualInterpreter.TryExecute("select * from information_schema.character_sets", EmptyCtx(), out var table));
            Assert.Single(table.Columns);
            Assert.Equal("character_set_name", table.Columns[0].Name);
            Assert.Single(table.Data);
            Assert.Equal("UTF8", DecodeCell(table, row: 0, column: 0));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Named_projection_over_character_sets_returns_single_column()
        {
            Assert.True(PgVirtualInterpreter.TryExecute("select character_set_name from information_schema.character_sets", EmptyCtx(), out var table));
            Assert.Single(table.Columns);
            Assert.Single(table.Data);
            Assert.Equal("UTF8", DecodeCell(table, row: 0, column: 0));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Schema_and_table_lookup_is_case_insensitive()
        {
            Assert.True(PgVirtualInterpreter.TryExecute("SELECT character_set_name FROM INFORMATION_SCHEMA.CHARACTER_SETS", EmptyCtx(), out var table));
            Assert.Single(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Where_equals_matching_value_returns_one_row()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select character_set_name from information_schema.character_sets where character_set_name = 'UTF8'",
                EmptyCtx(), out var table));
            Assert.Single(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Where_in_with_matching_value_returns_one_row()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select character_set_name from information_schema.character_sets where character_set_name in ('UTF8','LATIN1')",
                EmptyCtx(), out var table));
            Assert.Single(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Limit_zero_returns_empty_data()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select character_set_name from information_schema.character_sets limit 0",
                EmptyCtx(), out var table));
            Assert.Empty(table.Data);
        }
        
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Unknown_table_is_rejected()
        {
            Assert.False(PgVirtualInterpreter.TryExecute("select * from no_such.foo", EmptyCtx(), out var table));
            Assert.Null(table);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Group_by_is_rejected()
        {
            Assert.False(PgVirtualInterpreter.TryExecute(
                "select character_set_name from information_schema.character_sets group by character_set_name",
                EmptyCtx(), out var table));
            Assert.Null(table);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Self_join_with_trivial_on_produces_cartesian_row()
        {
            // JoinExecutor evaluates joins over non-empty sources. 1 row x 1 row = 1 row,
            // with `*` expanding to both sides -> 2 columns.
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select * from information_schema.character_sets a join information_schema.character_sets b on 1=1",
                EmptyCtx(), out var table));
            Assert.Equal(2, table.Columns.Count);
            Assert.Single(table.Data);
        }
        
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Information_schema_tables_with_null_db_returns_empty_rowset()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select table_schema from information_schema.tables",
                EmptyCtx(), out var table));
            Assert.Single(table.Columns);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Information_schema_tables_with_where_and_null_db_returns_empty_rowset()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select table_schema, table_name, table_type from information_schema.tables where table_type = 'BASE TABLE'",
                EmptyCtx(), out var table));
            Assert.Equal(3, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        // Multi-statement batches (Npgsql startup pair)

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Paired_version_and_current_setting_probe_merges_columns()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select version();select current_setting('max_index_keys')",
                EmptyCtx(), out var table));

            Assert.Equal(2, table.Columns.Count);
            Assert.Equal("version", table.Columns[0].Name);
            Assert.Equal("current_setting", table.Columns[1].Name);
            Assert.Single(table.Data);
            Assert.StartsWith("PostgreSQL", DecodeCell(table, row: 0, column: 0));
            Assert.Equal("32", DecodeCell(table, row: 0, column: 1));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Multi_statement_with_unsupported_statement_falls_through()
        {
            // First statement is fine; second targets an unknown table -> reject the batch.
            Assert.False(PgVirtualInterpreter.TryExecute(
                "select version(); select * from no_such.foo",
                EmptyCtx(), out var table));
            Assert.Null(table);
        }

        // PowerBI PK / FK metadata empty-join shapes

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void PrimaryKeyConstraints_query_returns_empty_rowset_with_4_columns()
        {
            const string sql =
                "select i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME as INDEX_NAME, ii.COLUMN_NAME, ii.ORDINAL_POSITION, " +
                "case when i.CONSTRAINT_TYPE = 'PRIMARY KEY' then 'Y' else 'N' end as PRIMARY_KEY\n" +
                "from INFORMATION_SCHEMA.table_constraints i inner join INFORMATION_SCHEMA.key_column_usage ii " +
                "on i.CONSTRAINT_NAME = ii.CONSTRAINT_NAME";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(4, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void FkCentric_TableSchema_query_returns_empty_rowset_with_6_columns()
        {
            const string sql =
                "select pkcol.COLUMN_NAME as PK_COLUMN_NAME, fkcol.TABLE_SCHEMA AS FK_TABLE_SCHEMA, " +
                "fkcol.TABLE_NAME AS FK_TABLE_NAME, fkcol.COLUMN_NAME as FK_COLUMN_NAME, " +
                "fkcol.ORDINAL_POSITION as ORDINAL, fkcon.CONSTRAINT_SCHEMA || '_' || fkcol.TABLE_NAME " +
                "from information_schema.key_column_usage pkcol " +
                "join information_schema.key_column_usage fkcol on 1=1 " +
                "join information_schema.table_constraints fkcon on 1=1";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(6, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void PkCentric_TableSchema_query_returns_empty_rowset_with_6_columns()
        {
            const string sql =
                "select pkcol.TABLE_SCHEMA AS PK_TABLE_SCHEMA, pkcol.TABLE_NAME AS PK_TABLE_NAME, " +
                "pkcol.COLUMN_NAME as PK_COLUMN_NAME, fkcol.COLUMN_NAME as FK_COLUMN_NAME, " +
                "fkcol.ORDINAL_POSITION as ORDINAL, fkcon.CONSTRAINT_SCHEMA || '_' || fkcol.TABLE_NAME " +
                "from information_schema.key_column_usage pkcol " +
                "join information_schema.key_column_usage fkcol on 1=1 " +
                "join information_schema.table_constraints fkcon on 1=1";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(6, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Current_user_no_parens_returns_username()
        {
            // `current_user` is a SQL keyword form (no parens). PG parses it as a SQLValueFunction
            // node rather than a FuncCall, so the evaluator needs a dedicated branch.
            var ctx = new VirtualQueryContext { Username = "root" };
            Assert.True(PgVirtualInterpreter.TryExecute("SELECT current_user", ctx, out var table));
            Assert.Single(table.Data);
            Assert.Equal("root", DecodeCell(table, 0, 0));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pg_roles_one_row_for_current_user()
        {
            var ctx = new VirtualQueryContext { Username = "root" };
            Assert.True(PgVirtualInterpreter.TryExecute("SELECT rolname FROM pg_catalog.pg_roles", ctx, out var table));
            Assert.Single(table.Data);
            Assert.Equal("root", DecodeCell(table, 0, 0));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pgadmin_role_probe_returns_row_with_no_signal_backend()
        {
            // pgAdmin's role-introspection probe - the most complex query we handle. Exercises:
            // pg_roles + pg_auth_members virtual tables, current_user(), correlated subqueries
            // (the WITH RECURSIVE body's WHERE references the outer `roles.oid`), ARRAY(subquery)
            // constructor, x = ANY(array) operator, WITH RECURSIVE CTE evaluation (terminates on
            // iteration 1 because pg_auth_members is empty).
            //
            // Expected: one row with can_signal_backend=false (the connected user has no role
            // hierarchy and therefore isn't a member of pg_signal_backend).
            const string sql = """
                SELECT
                            roles.oid as id, roles.rolname as name,
                            roles.rolsuper as is_superuser,
                            CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreaterole END as
                            can_create_role,
                            CASE WHEN roles.rolsuper THEN true
                            ELSE roles.rolcreatedb END as can_create_db,
                            CASE WHEN 'pg_signal_backend'=ANY(ARRAY(WITH RECURSIVE cte AS (
                            SELECT pg_roles.oid,pg_roles.rolname FROM pg_roles
                                WHERE pg_roles.oid = roles.oid
                            UNION ALL
                            SELECT m.roleid,pgr.rolname FROM cte cte_1
                                JOIN pg_auth_members m ON m.member = cte_1.oid
                                JOIN pg_roles pgr ON pgr.oid = m.roleid)
                            SELECT rolname  FROM cte)) THEN True
                            ELSE False END as can_signal_backend
                        FROM
                            pg_catalog.pg_roles as roles
                        WHERE
                            rolname = current_user
                """;

            var ctx = new VirtualQueryContext { Username = "root" };
            Assert.True(PgVirtualInterpreter.TryExecute(sql, ctx, out var table));
            Assert.Equal(6, table.Columns.Count);
            Assert.Equal("id", table.Columns[0].Name);
            Assert.Equal("name", table.Columns[1].Name);
            Assert.Equal("is_superuser", table.Columns[2].Name);
            Assert.Equal("can_create_role", table.Columns[3].Name);
            Assert.Equal("can_create_db", table.Columns[4].Name);
            Assert.Equal("can_signal_backend", table.Columns[5].Name);

            Assert.Single(table.Data);
            var span = table.Data[0].ColumnData.Span;
            Assert.Equal("root", DecodeCell(table, 0, 1));
            Assert.Equal("f", DecodeCell(table, 0, 5)); // can_signal_backend = false
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pgadmin_database_properties_query_shape_is_accepted()
        {
            // pgAdmin's "Querying view" database properties probe. Exercises: pg_get_userbyid,
            // array_to_string, extended current_setting keys (lc_collate, lc_ctype,
            // default_tablespace), datconnlimit/datacl on pg_database, nested scalar subqueries
            // around current_setting().
            const string sql = """
                SELECT
                db.oid AS did, db.oid, db.datname AS name, db.dattablespace AS spcoid,
                spcname, datallowconn, pg_catalog.pg_encoding_to_char(encoding) AS encoding,
                pg_catalog.pg_get_userbyid(datdba) AS datowner,
                (select pg_catalog.current_setting('lc_collate')) as datcollate,
                (select pg_catalog.current_setting('lc_ctype')) as datctype,
                datconnlimit,
                pg_catalog.has_database_privilege(db.oid, 'CREATE') AS cancreate,
                pg_catalog.current_setting('default_tablespace') AS default_tablespace,
                descr.description AS comments, db.datistemplate AS is_template,
                '' AS tblacl,
                '' AS seqacl,
                '' AS funcacl,
                pg_catalog.array_to_string(datacl::text[], ', ') AS acl
                FROM pg_catalog.pg_database db
                LEFT OUTER JOIN pg_catalog.pg_tablespace ta ON db.dattablespace=ta.OID
                LEFT OUTER JOIN pg_catalog.pg_shdescription descr ON (
                db.oid=descr.objoid AND descr.classoid='pg_database'::regclass
                )
                WHERE
                db.oid = 16384::OID
                ORDER BY datname
                """;

            Assert.True(PgVirtualInterpreter.TryExecute(sql, new VirtualQueryContext { Username = "root" }, out var table));
            Assert.Equal(19, table.Columns.Count);
            Assert.Equal("did", table.Columns[0].Name);
            Assert.Equal("acl", table.Columns[18].Name);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pgadmin_database_tree_load_query_shape_is_accepted()
        {
            // pgAdmin's database-tree-load probe. Exercises: LEFT JOIN against empty pg_tablespace
            // / pg_shdescription, qualified function name (pg_catalog.has_database_privilege),
            // type-cast unwrapping (16383::OID, 'pg_database'::regclass), IN-list, ORDER BY a
            // projected name.
            const string sql = """
                SELECT
                db.oid as did, db.datname as name, ta.spcname as spcname, db.datallowconn,
                db.datistemplate AS is_template,
                pg_catalog.has_database_privilege(db.oid, 'CREATE') as cancreate, datdba as owner,
                descr.description
                FROM
                pg_catalog.pg_database db
                LEFT OUTER JOIN pg_catalog.pg_tablespace ta ON db.dattablespace = ta.oid
                LEFT OUTER JOIN pg_catalog.pg_shdescription descr ON (
                db.oid=descr.objoid AND descr.classoid='pg_database'::regclass
                )
                WHERE db.oid > 16383::OID OR db.datname IN ('postgres', 'edb')

                ORDER BY datname
                """;

            // No DocumentDatabase wired in for this unit test - pg_database yields zero rows
            // without one. The point of this test is that the query *parses and dispatches*
            // through the interpreter; the row content is exercised end-to-end in EmbeddedTests.
            Assert.True(PgVirtualInterpreter.TryExecute(sql, new VirtualQueryContext { Username = "root" }, out var table));
            Assert.Equal(8, table.Columns.Count);
            Assert.Equal("did", table.Columns[0].Name);
            Assert.Equal("name", table.Columns[1].Name);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pgadmin_schema_tree_load_query_shape_is_accepted()
        {
            // pgAdmin's schema-tree-load probe. Exercises: LEFT JOIN against empty pg_description,
            // has_schema_privilege() function, EXISTS (SELECT 1 FROM pg_class WHERE ...) correlated
            // subqueries, deeply nested NOT (a OR b OR c) WHERE shape, LIKE with E'...' escape
            // string (NOT LIKE E'pg\_%' to hide internal pg_* schemas).
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

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(5, table.Columns.Count);
            Assert.Equal("oid", table.Columns[0].Name);
            Assert.Equal("name", table.Columns[1].Name);
            Assert.Equal("can_create", table.Columns[2].Name);
            Assert.Equal("has_usage", table.Columns[3].Name);
            Assert.Equal("description", table.Columns[4].Name);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pgadmin_database_probe_shape_is_accepted()
        {
            // pgAdmin's next probe after the replication check - reads `pg_database` for the
            // current DB. With no database in the test context the table yields no rows, but the
            // dispatch path (inline FuncCall via ExpressionEvaluator, pg_database virtual table,
            // current_database()/pg_encoding_to_char()/has_database_privilege() functions) must
            // accept the SQL and project six columns.
            const string sql = """
                SELECT
                    db.oid as did, db.datname, db.datallowconn,
                    pg_encoding_to_char(db.encoding) AS serverencoding,
                    has_database_privilege(db.oid, 'CREATE') as cancreate,
                    datistemplate
                FROM
                    pg_catalog.pg_database db
                WHERE db.datname = current_database()
                """;

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(6, table.Columns.Count);
            Assert.Equal("did", table.Columns[0].Name);
            Assert.Equal("datname", table.Columns[1].Name);
            Assert.Equal("serverencoding", table.Columns[3].Name);
            Assert.Equal("cancreate", table.Columns[4].Name);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pgadmin_replication_probe_returns_null_when_no_extensions_or_slots()
        {
            // pgAdmin sends this on connect to detect BDR / replication. RavenDB has neither -
            // pg_extension and pg_replication_slots are empty virtual tables, both COUNTs are 0,
            // and the CASE falls through to ELSE NULL. Exercises: no-FROM expression path, scalar
            // subqueries, COUNT aggregate without GROUP BY.
            const string sql = """
                SELECT CASE
                WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname='bdr') > 0
                THEN 'pgd'
                WHEN (SELECT COUNT(*) FROM pg_replication_slots) > 0
                THEN 'log'
                ELSE NULL
                END as type
                """;

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Single(table.Columns);
            Assert.Equal("type", table.Columns[0].Name);
            Assert.Single(table.Data);
            var cell = table.Data[0].ColumnData.Span[0];
            Assert.False(cell.HasValue, "no extensions or replication slots -> NULL");
        }

        // PowerBI information_schema.tables / .columns probes

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void PowerBI_all_collections_probe_accepted_with_3_columns()
        {
            // Every variant of the all-collections probe (different WHERE filters, aliased columns,
            // reordered columns) goes through the interpreter against InformationSchemaTablesTable.
            // With no database the row set is empty, but the schema must still come through cleanly.
            const string sql =
                "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\n" +
                "from INFORMATION_SCHEMA.tables\n" +
                "where TABLE_SCHEMA not in ('information_schema', 'pg_catalog')\n" +
                "order by TABLE_SCHEMA, TABLE_NAME";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(3, table.Columns.Count);
            // PG case-folds unquoted identifiers - TABLE_SCHEMA reaches us as table_schema.
            Assert.Equal("table_schema", table.Columns[0].Name);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void PowerBI_preview_columns_probe_accepted_with_4_columns()
        {
            // The columns probe with TABLE_NAME='X' equality. With no database the table
            // introspection returns nothing (column schema still present).
            const string sql =
                "select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE\n" +
                "from INFORMATION_SCHEMA.columns\n" +
                "where TABLE_SCHEMA = 'public' and TABLE_NAME = 'Regions'\n" +
                "order by ORDINAL_POSITION";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(4, table.Columns.Count);
            Assert.Equal("column_name", table.Columns[0].Name);
            Assert.Equal("ordinal_position", table.Columns[1].Name);
            Assert.Equal("is_nullable", table.Columns[2].Name);
            Assert.Equal("data_type", table.Columns[3].Name);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void PowerBI_preview_columns_subset_projection_accepted()
        {
            // The interpreter accepts column subsets: virtual tables expose all 4 and projection
            // picks what's asked for.
            const string sql =
                "select COLUMN_NAME from INFORMATION_SCHEMA.columns where TABLE_NAME = 'Products'";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Single(table.Columns);
            Assert.Equal("column_name", table.Columns[0].Name);
        }

        // PowerBI Desktop's data-loader fires this shape against every collection it's about to
        // import - it's the gating call before any actual SELECT. Exercises CASE in the projection,
        // LIKE pattern matching against the column's data_type, string concatenation with ||, and
        // ORDER BY on columns not in the SELECT list (TABLE_SCHEMA/TABLE_NAME come from the
        // underlying row schema). With no database the row scan yields nothing, but the query must
        // still be *accepted* by the interpreter (otherwise PowerBI sees `Unhandled query`).
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void PowerBI_data_loader_columns_probe_with_case_like_and_concat_accepted()
        {
            const string sql =
                "select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, " +
                "case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE " +
                "from INFORMATION_SCHEMA.columns " +
                "where TABLE_SCHEMA = 'public' and TABLE_NAME = 'Categories' " +
                "order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(4, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        // pgAdmin's per-result-column type-name probe: it batches up oids from a result set and
        // asks "what's the canonical PG type name for each?" via a parameterized array. The
        // parameter ($1) isn't bound at interpret time (we run at Parse-time in the extended
        // protocol), so ParamRef resolves to NULL and the WHERE filters everything out. The query
        // must still be accepted by the interpreter with the right column shape - pgAdmin's data
        // grid then just shows raw oids instead of friendly names, but the query window stays open.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void PgAdmin_format_type_type_introspection_probe_accepted_empty()
        {
            const string sql =
                "SELECT oid, pg_catalog.format_type(oid, NULL) AS typname " +
                "FROM pg_catalog.pg_type WHERE oid = ANY($1) ORDER BY oid";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(2, table.Columns.Count);
            Assert.Equal("oid", table.Columns[0].Name);
            Assert.Equal("typname", table.Columns[1].Name);
            Assert.Empty(table.Data);
        }

        // Npgsql pg_catalog metadata empty-join shapes

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void EnumTypes_query_returns_empty_rowset_with_2_columns()
        {
            const string sql =
                "SELECT pg_type.oid, enumlabel FROM pg_enum JOIN pg_type ON pg_type.oid = enumtypid ORDER BY oid";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(2, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void CompositeTypes_query_returns_empty_rowset_with_3_columns()
        {
            const string sql =
                "SELECT typ.oid, att.attname, att.atttypid " +
                "FROM pg_type AS typ " +
                "JOIN pg_namespace AS ns ON ns.oid = typ.typnamespace " +
                "JOIN pg_class AS cls ON cls.oid = typ.typrelid " +
                "JOIN pg_attribute AS att ON att.attrelid = typ.typrelid " +
                "WHERE typ.typtype = 'c'";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(3, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Unknown_join_table_rejects_the_query()
        {
            const string sql =
                "SELECT pg_type.oid, enumlabel FROM pg_enum JOIN no_such.foo ON 1=1";

            Assert.False(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Null(table);
        }

        // PowerBI ReferentialConstraints FK metadata (sub-FROM shape)

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ReferentialConstraints_FkCentric_query_returns_empty_rowset_with_6_columns()
        {
            const string sql =
                "select pkcol.COLUMN_NAME as PK_COLUMN_NAME, fkcol.TABLE_SCHEMA AS FK_TABLE_SCHEMA, " +
                "fkcol.TABLE_NAME AS FK_TABLE_NAME, fkcol.COLUMN_NAME as FK_COLUMN_NAME, " +
                "fkcol.ORDINAL_POSITION as ORDINAL, " +
                "fkcon.CONSTRAINT_SCHEMA || '*' || fkcol.TABLE_NAME || '_' || fkcon.CONSTRAINT_NAME as FK_NAME\n" +
                "from\n" +
                "(select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, unique_constraint_name " +
                "from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon\n" +
                "inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME\n" +
                "inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME\n" +
                "where pkcol.TABLE_SCHEMA = 'public' and pkcol.TABLE_NAME = 'Employees' and pkcol.ORDINAL_POSITION = fkcol.ORDINAL_POSITION\n" +
                "order by FK_NAME, fkcol.ORDINAL_POSITION";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(6, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ReferentialConstraints_PkCentric_query_returns_empty_rowset_with_6_columns()
        {
            const string sql =
                "select pkcol.TABLE_SCHEMA AS PK_TABLE_SCHEMA, pkcol.TABLE_NAME AS PK_TABLE_NAME, " +
                "pkcol.COLUMN_NAME as PK_COLUMN_NAME, fkcol.COLUMN_NAME as FK_COLUMN_NAME, " +
                "fkcol.ORDINAL_POSITION as ORDINAL, " +
                "fkcon.CONSTRAINT_SCHEMA || '*' || pkcol.TABLE_NAME || '_' || fkcon.CONSTRAINT_NAME as FK_NAME\n" +
                "from\n" +
                "(select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, unique_constraint_name " +
                "from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon\n" +
                "inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME\n" +
                "inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(6, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Sub_from_with_non_empty_inner_source_is_materialized()
        {
            // Recursive sub-FROM evaluation: the inner SELECT runs against the real (non-empty)
            // information_schema.character_sets table, then the outer references its
            // alias-prefixed column.
            const string sql =
                "select cs.character_set_name from (select character_set_name from information_schema.character_sets) cs";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Single(table.Columns);
            Assert.Single(table.Data);
            Assert.Equal("UTF8", DecodeCell(table, row: 0, column: 0));
        }

        // pg_catalog data backs the Npgsql type-loading queries via the interpreter

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Select_oid_typname_from_pg_type_returns_rows()
        {
            Assert.True(PgVirtualInterpreter.TryExecute("select oid, typname from pg_type", EmptyCtx(), out var table));
            Assert.Equal(2, table.Columns.Count);
            Assert.NotEmpty(table.Data);
            Assert.Equal("oid", table.Columns[0].Name);
            Assert.Equal("typname", table.Columns[1].Name);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Select_with_join_pg_namespace_resolves_nspname()
        {
            const string sql =
                "select ns.nspname, a.typname from pg_type as a join pg_namespace as ns on ns.oid = a.typnamespace";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(2, table.Columns.Count);
            Assert.Equal("nspname", table.Columns[0].Name);
            Assert.NotEmpty(table.Data);

            // Every projected row should have a non-null nspname now that the join resolves.
            foreach (var row in table.Data)
            {
                var cell = row.ColumnData.Span[0];
                Assert.True(cell.HasValue, "nspname should be non-null after the inner JOIN");
            }
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Case_when_in_projection_classifies_arrays()
        {
            // The variant queries reduce array detection to a CASE WHEN over pg_proc.proname; the
            // interpreter needs to evaluate the CASE per-row and produce 'a' for arrays.
            const string sql =
                "select a.oid, case when pg_proc.proname = 'array_recv' then 'a' else a.typtype end as type " +
                "from pg_type as a join pg_proc on pg_proc.oid = a.typreceive";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(2, table.Columns.Count);

            // At least one 'a' should appear (every array type's CASE branch is taken).
            var sawArray = false;
            foreach (var row in table.Data)
            {
                var typeCell = row.ColumnData.Span[1];
                if (typeCell.HasValue && Encoding.UTF8.GetString(typeCell.Value.Span) == "a")
                {
                    sawArray = true;
                    break;
                }
            }
            Assert.True(sawArray, "expected CASE WHEN to map at least one row to 'a' (array)");
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Npgsql3_legacy_types_query_runs_through_interpreter()
        {
            // Npgsql 3.2.x type-loader query. Five-source join (pg_type + pg_namespace +
            // pg_proc + LEFT OUTER pg_type + LEFT OUTER pg_range) with CASE WHEN projection,
            // OR-of-AND WHERE, and ORDER BY a projected alias.
            const string sql = """
                SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,
                CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,
                CASE
                  WHEN pg_proc.proname='array_recv' THEN a.typelem
                  WHEN a.typtype='r' THEN rngsubtype
                  ELSE 0
                END AS elemoid,
                CASE
                  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3
                  WHEN a.typtype='r' THEN 2
                  WHEN a.typtype='d' THEN 1
                  ELSE 0
                END AS ord
                FROM pg_type AS a
                JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
                JOIN pg_proc ON pg_proc.oid = a.typreceive
                LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)
                LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid)
                WHERE
                  (
                    a.typtype IN ('b', 'r', 'e', 'd') AND
                    (b.typtype IS NULL OR b.typtype IN ('b', 'r', 'e', 'd'))
                  ) OR
                  (a.typname IN ('record', 'void') AND a.typtype = 'p')
                ORDER BY ord
                """;

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(8, table.Columns.Count);
            Assert.Equal("nspname", table.Columns[0].Name);
            Assert.Equal("typname", table.Columns[1].Name);
            Assert.Equal("oid", table.Columns[2].Name);
            Assert.Equal("type", table.Columns[5].Name);
            Assert.Equal("elemoid", table.Columns[6].Name);
            Assert.Equal("ord", table.Columns[7].Name);
            Assert.NotEmpty(table.Data);

            // Spot-check: int4 (oid=23) must appear as a base type.
            var sawInt4 = false;
            foreach (var row in table.Data)
            {
                var span = row.ColumnData.Span;
                if (span[2].HasValue && Encoding.UTF8.GetString(span[2].Value.Span) == "23" &&
                    span[1].HasValue && Encoding.UTF8.GetString(span[1].Value.Span) == "int4" &&
                    span[5].HasValue && Encoding.UTF8.GetString(span[5].Value.Span) == "b")
                {
                    sawInt4 = true;
                    break;
                }
            }
            Assert.True(sawInt4, "expected int4 (oid=23) to be present as a base type ('b')");

            // ord must be non-decreasing (ORDER BY ord).
            int prev = int.MinValue;
            foreach (var row in table.Data)
            {
                var cell = row.ColumnData.Span[7];
                Assert.True(cell.HasValue);
                var v = int.Parse(Encoding.UTF8.GetString(cell.Value.Span));
                Assert.True(v >= prev, $"ord must be non-decreasing: got {v} after {prev}");
                prev = v;
            }
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Npgsql4_0_x_old_flat_types_query_runs_through_interpreter()
        {
            // The Npgsql 4.0.x type-loader shape: joins pg_class on top of V3's sources and uses
            // an OR-of-AND WHERE with nested ORs / IN-lists / pg_proc-on-array_recv guarded blocks.
            const string sql =
                @"/*** Load all supported types ***/
SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,
CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,
CASE
  WHEN pg_proc.proname='array_recv' THEN a.typelem
  WHEN a.typtype='r' THEN rngsubtype
  ELSE 0
END AS elemoid,
CASE
  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3
  WHEN a.typtype='r' THEN 2
  WHEN a.typtype='d' THEN 1
  ELSE 0
END AS ord
FROM pg_type AS a
JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
JOIN pg_proc ON pg_proc.oid = a.typreceive
LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)
LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)
LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)
LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid)
WHERE
  a.typtype IN ('b', 'r', 'e', 'd') OR
  (a.typtype = 'c' AND cls.relkind='c') OR
  (pg_proc.proname='array_recv' AND (
    b.typtype IN ('b', 'r', 'e', 'd') OR
    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR
    (b.typtype = 'c' AND elemcls.relkind='c')
  )) OR
  (a.typtype = 'p' AND a.typname IN ('record', 'void'))
ORDER BY ord";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(8, table.Columns.Count);
            Assert.NotEmpty(table.Data);

            int prev = int.MinValue;
            foreach (var row in table.Data)
            {
                var cell = row.ColumnData.Span[7];
                Assert.True(cell.HasValue);
                var v = int.Parse(Encoding.UTF8.GetString(cell.Value.Span));
                Assert.True(v >= prev);
                prev = v;
            }
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Npgsql4_1_2_mid_flat_types_query_runs_through_interpreter()
        {
            // MidFlat (Npgsql 4.1.0-4.1.2) is OldFlat + a typcategory branch in the ord CASE.
            // Requires pg_type.typcategory to be present.
            const string sql =
                @"
/*** Load all supported types ***/
SELECT ns.nspname, a.typname, a.oid, a.typbasetype,
CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS typtype,
CASE
  WHEN pg_proc.proname='array_recv' THEN a.typelem
  WHEN a.typtype='r' THEN rngsubtype
  ELSE 0
END AS typelem,
CASE
  WHEN a.typtype='d' AND a.typcategory='A' THEN 4
  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3
  WHEN a.typtype='r' THEN 2
  WHEN a.typtype='d' THEN 1
  ELSE 0
END AS ord
FROM pg_type AS a
JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
JOIN pg_proc ON pg_proc.oid = a.typreceive
LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)
LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)
LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)
LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid)
WHERE
  a.typtype IN ('b', 'r', 'e', 'd') OR
  (a.typtype = 'c' AND cls.relkind='c') OR
  (pg_proc.proname='array_recv' AND (
    b.typtype IN ('b', 'r', 'e', 'd') OR
    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR
    (b.typtype = 'c' AND elemcls.relkind='c')
  )) OR
  (a.typtype = 'p' AND a.typname IN ('record', 'void'))
ORDER BY ord";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(7, table.Columns.Count);
            Assert.Equal("typelem", table.Columns[5].Name);
            Assert.Equal("ord", table.Columns[6].Name);
            Assert.NotEmpty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Npgsql5_modern_nested_types_query_runs_through_interpreter()
        {
            // The full Modern Nested shape: three-level FROM, two outer LEFT JOINs at each level,
            // qualified-wildcard `typ_and_elem_type.*` projection, an outer CASE WHEN producing ord.
            // Exercises recursive sub-FROM execution and alias.* propagation.
            const string sql =
                @"SELECT ns.nspname, typ_and_elem_type.*,
   CASE
       WHEN typtype IN ('b', 'e', 'p') THEN 0
       WHEN typtype = 'r' THEN 1
       WHEN typtype = 'c' THEN 2
       WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 3
       WHEN typtype = 'a' THEN 4
       WHEN typtype = 'd' AND elemtyptype = 'a' THEN 5
    END AS ord
FROM (
    SELECT
        typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,
        elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,
        CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype
    FROM (
        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,
            CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,
            CASE
                WHEN proc.proname='array_recv' THEN typ.typelem
                WHEN typ.typtype='r' THEN rngsubtype
                WHEN typ.typtype='d' THEN typ.typbasetype
            END AS elemtypoid
        FROM pg_type AS typ
        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
        LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive
        LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)
    ) AS typ
    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid
    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)
    LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive
) AS typ_and_elem_type
JOIN pg_namespace AS ns ON (ns.oid = typnamespace)
WHERE
    typtype IN ('b', 'r', 'e', 'd') OR
    (typtype = 'c' AND relkind='c') OR
    (typtype = 'p' AND typname IN ('record', 'void')) OR
    (typtype = 'a' AND (
        elemtyptype IN ('b', 'r', 'e', 'd') OR
        (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR
        (elemtyptype = 'c' AND elemrelkind='c')
    ))
ORDER BY ord";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            // 1 (ns.nspname) + 11 (typ_and_elem_type's 11 inner columns) + 1 (ord) = 13.
            Assert.Equal(13, table.Columns.Count);
            Assert.Equal("nspname", table.Columns[0].Name);
            Assert.Equal("ord", table.Columns[12].Name);
            Assert.NotEmpty(table.Data);
            
            var emptyCells = new System.Collections.Generic.List<string>();
            for (int rowIdx = 0; rowIdx < table.Data.Count; rowIdx++)
            {
                var span = table.Data[rowIdx].ColumnData.Span;
                for (int col = 0; col < table.Columns.Count; col++)
                {
                    var cell = span[col];
                    if (cell.HasValue == false)
                        continue;
                    if (cell.Value.Length == 0)
                        emptyCells.Add($"row {rowIdx} col '{table.Columns[col].Name}' (typename row 0='{DecodeCell(table, 0, 3)}')");
                }
                if (emptyCells.Count >= 3) break;
            }
            Assert.True(emptyCells.Count == 0, "empty non-null cells: " + string.Join("; ", emptyCells));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Order_by_projected_alias_sorts_after_projection()
        {
            // ORDER BY ord references a CASE WHEN-derived column - the sort must run after the
            // projection materializes its values, not against the original FROM source.
            const string sql =
                "select a.oid, case when a.typtype = 'r' then 2 else 0 end as ord " +
                "from pg_type as a order by ord";

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.NotEmpty(table.Data);

            int prev = int.MinValue;
            foreach (var row in table.Data)
            {
                var cell = row.ColumnData.Span[1];
                Assert.True(cell.HasValue);
                var v = int.Parse(Encoding.UTF8.GetString(cell.Value.Span));
                Assert.True(v >= prev, "rows must be in non-decreasing ord order");
                prev = v;
            }
        }

        // Microsoft Fabric Copy Job / Npgsql 6+ compact type-loading query
        // RavenDB-26024: Fabric's Copy Job connector sends a two-statement Simple Query
        // batch (`SELECT version(); SELECT ns.nspname, ...`) whose second statement is the
        // Npgsql 6+ compact type-discovery shape. It is a single outer SELECT with a
        // sub-FROM inner query (one level deep, unlike Npgsql 5's two-level modern nested).

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Npgsql6_compact_types_query_runs_through_interpreter()
        {
            // The compact Npgsql 6+ / Microsoft Fabric Copy Job type-loading query.
            // Outer SELECT projects 6 columns; inner subquery wraps pg_type + LEFT JOINs,
            // computes typtype/elemtypoid via CASE WHEN, and joins pg_namespace on the outside.
            const string sql = """
                SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
                FROM (
                    SELECT
                        t.oid, t.typnamespace, t.typname, t.typnotnull,
                        CASE WHEN proc.proname = 'array_recv' THEN 'a' ELSE t.typtype END AS typtype,
                        CASE
                            WHEN proc.proname = 'array_recv' THEN t.typelem
                            WHEN t.typtype = 'r' THEN rngsubtype
                            WHEN t.typtype = 'd' THEN t.typbasetype
                        END AS elemtypoid
                    FROM pg_type AS t
                    LEFT JOIN pg_class AS cls ON (cls.oid = t.typrelid)
                    LEFT JOIN pg_proc AS proc ON (proc.oid = t.typreceive)
                    LEFT JOIN pg_range ON (pg_range.rngtypid = t.oid)
                ) AS t
                JOIN pg_namespace AS ns ON (ns.oid = t.typnamespace)
                WHERE
                    t.typtype IN ('b', 'r', 'e', 'd') OR
                    t.typtype = 'c' OR
                    (t.typtype = 'p' AND t.typname IN ('record', 'void')) OR
                    (t.typtype = 'a' AND t.typname NOT LIKE '\_\_%')
                ORDER BY t.oid
                """;

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(6, table.Columns.Count);
            Assert.Equal("nspname",    table.Columns[0].Name);
            Assert.Equal("oid",        table.Columns[1].Name);
            Assert.Equal("typname",    table.Columns[2].Name);
            Assert.Equal("typtype",    table.Columns[3].Name);
            Assert.Equal("typnotnull", table.Columns[4].Name);
            Assert.Equal("elemtypoid", table.Columns[5].Name);
            Assert.NotEmpty(table.Data);

            // int4 (oid=23) must appear as a base type 'b'.
            var sawInt4 = false;
            foreach (var row in table.Data)
            {
                var span = row.ColumnData.Span;
                if (span[1].HasValue && Encoding.UTF8.GetString(span[1].Value.Span) == "23" &&
                    span[3].HasValue && Encoding.UTF8.GetString(span[3].Value.Span) == "b")
                {
                    sawInt4 = true;
                    break;
                }
            }
            Assert.True(sawInt4, "int4 (oid=23) must appear as typtype='b'");
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Fabric_copy_job_full_batch_second_statement_is_type_query()
        {
            // The Fabric Copy Job sends the full `SELECT version(); SELECT ns.nspname ...`
            // batch in a single message. After splitting, statement 2 must succeed through
            // the interpreter. Validates that SqlStatementSplitter correctly isolates the
            // second statement so the interpreter can handle it.
            const string typeSql = """
                SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
                FROM (
                    SELECT
                        t.oid, t.typnamespace, t.typname, t.typnotnull,
                        CASE WHEN proc.proname = 'array_recv' THEN 'a' ELSE t.typtype END AS typtype,
                        CASE
                            WHEN proc.proname = 'array_recv' THEN t.typelem
                            WHEN t.typtype = 'r' THEN rngsubtype
                            WHEN t.typtype = 'd' THEN t.typbasetype
                        END AS elemtypoid
                    FROM pg_type AS t
                    LEFT JOIN pg_class AS cls ON (cls.oid = t.typrelid)
                    LEFT JOIN pg_proc AS proc ON (proc.oid = t.typreceive)
                    LEFT JOIN pg_range ON (pg_range.rngtypid = t.oid)
                ) AS t
                JOIN pg_namespace AS ns ON (ns.oid = t.typnamespace)
                WHERE
                    t.typtype IN ('b', 'r', 'e', 'd') OR
                    t.typtype = 'c' OR
                    (t.typtype = 'p' AND t.typname IN ('record', 'void')) OR
                    (t.typtype = 'a' AND t.typname NOT LIKE '\_\_%')
                ORDER BY t.oid
                """;

            // Statement 2 must succeed on its own - that is what `Query.cs` dispatches after splitting.
            Assert.True(PgVirtualInterpreter.TryExecute(typeSql, EmptyCtx(), out var table));
            Assert.Equal(6, table.Columns.Count);
            Assert.NotEmpty(table.Data);
        }

        // Npgsql 7.x / 8.x type-loading query
        // Npgsql 7+ kept the two-level nested structure from 4.1.3-5.x ModernNested but
        // added `'m'` (multirange) to the typtype IN list (introduced in PG 14) and broke
        // the ord CASE into more buckets. Our pg_type CSV doesn't carry multirange rows, so
        // they produce zero matches; the structural shape is what we're validating.

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Npgsql7_modern_nested_with_multirange_filter_runs_through_interpreter()
        {
            const string sql = """
                SELECT ns.nspname, typ_and_elem_type.*,
                   CASE
                       WHEN typtype IN ('b', 'e', 'p') THEN 0
                       WHEN typtype = 'r' THEN 1
                       WHEN typtype = 'm' THEN 2
                       WHEN typtype = 'c' THEN 3
                       WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 4
                       WHEN typtype = 'a' THEN 5
                       WHEN typtype = 'd' AND elemtyptype = 'a' THEN 6
                    END AS ord
                FROM (
                    SELECT
                        typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,
                        elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,
                        CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype
                    FROM (
                        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,
                            CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,
                            CASE
                                WHEN proc.proname='array_recv' THEN typ.typelem
                                WHEN typ.typtype='r' THEN rngsubtype
                                WHEN typ.typtype='d' THEN typ.typbasetype
                            END AS elemtypoid
                        FROM pg_type AS typ
                        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
                        LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive
                        LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)
                    ) AS typ
                    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid
                    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)
                    LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive
                ) AS typ_and_elem_type
                JOIN pg_namespace AS ns ON (ns.oid = typnamespace)
                WHERE
                    typtype IN ('b', 'r', 'm', 'e', 'd') OR
                    (typtype = 'c' AND relkind='c') OR
                    (typtype = 'p' AND typname IN ('record', 'void')) OR
                    (typtype = 'a' AND (
                        elemtyptype IN ('b', 'r', 'm', 'e', 'd') OR
                        (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR
                        (elemtyptype = 'c' AND elemrelkind='c')
                    ))
                ORDER BY ord
                """;

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            // 1 (ns.nspname) + 11 (typ_and_elem_type's columns) + 1 (ord) = 13.
            Assert.Equal(13, table.Columns.Count);
            Assert.Equal("nspname", table.Columns[0].Name);
            Assert.Equal("ord", table.Columns[12].Name);
            Assert.NotEmpty(table.Data);

            // The `ord` column must be non-decreasing across rows - Npgsql relies on that
            // order to set up its type-resolution sequence (base types must resolve before
            // arrays of those base types).
            int prev = int.MinValue;
            foreach (var row in table.Data)
            {
                var cell = row.ColumnData.Span[12];
                Assert.True(cell.HasValue, "ord must be non-null");
                var v = int.Parse(Encoding.UTF8.GetString(cell.Value.Span));
                Assert.True(v >= prev, "rows must be in non-decreasing ord order");
                prev = v;
            }
        }

        // Npgsql 8.x is structurally identical to 7.x for the type-loading query - same
        // multirange branching, same ord categorization. The version-bump didn't change
        // the query shape, but pinning it here makes intent obvious and gives us a
        // regression net if Npgsql 8.x ever quietly changes.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Npgsql8_compatible_types_query_is_accepted_same_as_7x()
        {
            // Same shape, just using a slightly different surface trick - Npgsql 8's
            // GenerateLoadTypesQuery sometimes adds an explicit `t.typname NOT LIKE '\_\_%'`
            // guard on array types to skip system-internal pseudo-arrays. Mirrors the
            // Fabric compact shape's LIKE clause; verifies our LIKE handles escaped chars.
            const string sql = """
                SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
                FROM (
                    SELECT
                        t.oid, t.typnamespace, t.typname, t.typnotnull,
                        CASE WHEN proc.proname = 'array_recv' THEN 'a' ELSE t.typtype END AS typtype,
                        CASE
                            WHEN proc.proname = 'array_recv' THEN t.typelem
                            WHEN t.typtype = 'r' THEN rngsubtype
                            WHEN t.typtype = 'm' THEN rngtypid
                            WHEN t.typtype = 'd' THEN t.typbasetype
                        END AS elemtypoid
                    FROM pg_type AS t
                    LEFT JOIN pg_class AS cls ON (cls.oid = t.typrelid)
                    LEFT JOIN pg_proc AS proc ON (proc.oid = t.typreceive)
                    LEFT JOIN pg_range ON (pg_range.rngtypid = t.oid)
                ) AS t
                JOIN pg_namespace AS ns ON (ns.oid = t.typnamespace)
                WHERE
                    t.typtype IN ('b', 'r', 'm', 'e', 'd') OR
                    t.typtype = 'c' OR
                    (t.typtype = 'p' AND t.typname IN ('record', 'void')) OR
                    (t.typtype = 'a' AND t.typname NOT LIKE '\_\_%')
                ORDER BY t.oid
                """;

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(6, table.Columns.Count);
            Assert.NotEmpty(table.Data);
        }

        // Npgsql 9.x / 10.x type-loading query
        // Npgsql 9 / 10 added on top of the 7/8 ModernNested shape:
        //   1. `typ.typcategory` column projected through both subquery levels.
        //   2. A CORRELATED SCALAR SUBQUERY inside a CASE branch - the multirange case
        //      reads `(SELECT rngtypid FROM pg_range WHERE rngmultitypid = typ.oid)` to
        //      resolve the underlying range type for a multirange. This exercises both
        //      correlated-subquery resolution and scalar-subquery-as-a-value evaluation
        //      in combination, inside CASE, inside a projection of a sub-FROM.
        //   3. Refined WHERE structure with `typcategory = 'U'` and schema-conditional
        //      composite/array branches.
        //   4. `'p'` typtype now includes 'unknown' alongside 'record' and 'void'.

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Npgsql9_modern_nested_with_multirange_subquery_runs_through_interpreter()
        {
            const string sql = """
                SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
                FROM (
                    SELECT
                        typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,
                        elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,
                        CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype,
                        typ.typcategory
                    FROM (
                        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,
                            CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,
                            CASE
                                WHEN proc.proname='array_recv' THEN typ.typelem
                                WHEN typ.typtype='r' THEN rngsubtype
                                WHEN typ.typtype='m' THEN (SELECT rngtypid FROM pg_range WHERE rngmultitypid = typ.oid)
                                WHEN typ.typtype='d' THEN typ.typbasetype
                            END AS elemtypoid,
                            typ.typcategory
                        FROM pg_type AS typ
                        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
                        LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive
                        LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)
                    ) AS typ
                    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid
                    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)
                    LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive
                ) AS t
                JOIN pg_namespace AS ns ON (ns.oid = typnamespace)
                WHERE
                    (ns.nspname IN ('pg_catalog', 'information_schema', 'pg_toast') OR typcategory = 'U') AND (
                    typtype IN ('b', 'r', 'm', 'e', 'd') OR
                    (typtype = 'c' AND ns.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')) OR
                    (typtype = 'p' AND typname IN ('record', 'void', 'unknown')) OR
                    (typtype = 'a' AND (
                        elemtyptype IN ('b', 'r', 'm', 'e', 'd') OR
                        (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR
                        (elemtyptype = 'c' AND ns.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast'))
                    )))
                ORDER BY CASE
                       WHEN typtype IN ('b', 'e', 'p') THEN 0
                       WHEN typtype = 'c' THEN 1
                       WHEN typtype = 'r' THEN 2
                       WHEN typtype = 'm' THEN 3
                       WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 4
                       WHEN typtype = 'a' THEN 5
                       WHEN typtype = 'd' AND elemtyptype = 'a' THEN 6
                END
                """;

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(6, table.Columns.Count);
            Assert.Equal("nspname",    table.Columns[0].Name);
            Assert.Equal("oid",        table.Columns[1].Name);
            Assert.Equal("typname",    table.Columns[2].Name);
            Assert.Equal("typtype",    table.Columns[3].Name);
            Assert.Equal("typnotnull", table.Columns[4].Name);
            Assert.Equal("elemtypoid", table.Columns[5].Name);
            Assert.NotEmpty(table.Data);
        }

        // Microsoft Fabric Copy Job's "Choose data" picker sends this UNION between the tables
        // and (empty) views catalogs. Before top-level UNION support + InformationSchemaViewsTable
        // registration, this dispatched all the way through to "Unhandled query". Pins both fixes.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Fabric_copy_job_tables_union_views_shape_is_accepted()
        {
            const string sql = """
                select table_schema,table_name from information_schema.tables where table_schema !='pg_catalog'
                union
                select table_schema,table_name from information_schema.views where table_schema !='pg_catalog'
                order by table_schema,table_name
                """;

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(2, table.Columns.Count);
            Assert.Equal("table_schema", table.Columns[0].Name);
            Assert.Equal("table_name", table.Columns[1].Name);
            // No DocumentDatabase wired in here - InformationSchemaTablesTable yields zero rows,
            // and views is always empty, so the UNION produces no data. The point is that the
            // SHAPE is recognized and dispatched.
        }

        // UNION ALL must preserve duplicates whereas UNION dedupes. Smoke-test against two
        // arms that both produce a known catalog row (any oid present in pg_class works).
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Union_all_keeps_duplicates_union_dedupes()
        {
            const string unionDistinctSql = """
                select 'a' as col
                union
                select 'a' as col
                """;
            const string unionAllSql = """
                select 'a' as col
                union all
                select 'a' as col
                """;

            Assert.True(PgVirtualInterpreter.TryExecute(unionDistinctSql, EmptyCtx(), out var distinctTable));
            Assert.Single(distinctTable.Data);

            Assert.True(PgVirtualInterpreter.TryExecute(unionAllSql, EmptyCtx(), out var allTable));
            Assert.Equal(2, allTable.Data.Count);
        }

        // Outer ORDER BY on a UNION result must sort the COMBINED rows, not each arm.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Union_outer_order_by_sorts_combined_result()
        {
            const string sql = """
                select 'b' as col
                union
                select 'a' as col
                order by col
                """;

            Assert.True(PgVirtualInterpreter.TryExecute(sql, EmptyCtx(), out var table));
            Assert.Equal(2, table.Data.Count);
            Assert.Equal("a", DecodeCell(table, row: 0, column: 0));
            Assert.Equal("b", DecodeCell(table, row: 1, column: 0));
        }

        // count(DISTINCT x) isn't implemented - it must fall through, not silently return the
        // non-distinct count(x). Plain count is still handled.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Count_distinct_falls_through_instead_of_counting_non_distinct()
        {
            Assert.False(PgVirtualInterpreter.TryExecute("select count(distinct typname) from pg_type", EmptyCtx(), out _));
            Assert.True(PgVirtualInterpreter.TryExecute("select count(typname) from pg_type", EmptyCtx(), out _));
        }

        // ORDER BY over a column with mixed runtime types must sort without throwing. {2, 10, "15x"} is an
        // intransitivity witness for naive numeric-or-ordinal comparison; List.Sort (introsort) throws
        // "IComparer.Compare() inconsistent results" unless CompareCells is a total order. A large mixed
        // list forces the quicksort path that detects inconsistency.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void CompareCells_is_a_total_order_across_mixed_types()
        {
            var method = typeof(PgVirtualInterpreter).GetMethod("CompareCells", BindingFlags.NonPublic | BindingFlags.Static);
            Assert.NotNull(method);
            Comparison<object> cmp = (a, b) => (int)method.Invoke(null, new[] { a, b });

            var items = new List<object>();
            for (var i = 0; i < 5; i++)
            {
                items.Add(2L); items.Add(10L); items.Add("15x"); items.Add("abc");
                items.Add(7); items.Add(3.5); items.Add(null); items.Add(true); items.Add("10");
            }

            items.Sort(cmp); // must not throw

            for (var i = 1; i < items.Count; i++)
                Assert.True(cmp(items[i - 1], items[i]) <= 0);
        }

        // The LIKE regex must carry a finite MatchTimeout; a real timeout can't be tripped (the generated
        // regex is linear), so assert the cap is wired rather than try to trip it.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Like_regex_carries_a_match_timeout()
        {
            var method = typeof(ExpressionEvaluator).GetMethod("BuildLikeRegex", BindingFlags.NonPublic | BindingFlags.Static);
            Assert.NotNull(method);
            var regex = (Regex)method.Invoke(null, new object[] { "abc%def_", false });
            Assert.NotEqual(Regex.InfiniteMatchTimeout, regex.MatchTimeout);
        }

        private static VirtualQueryContext EmptyCtx() => new();

        private static string DecodeCell(Raven.Server.Integrations.PostgreSQL.Messages.PgTable table, int row, int column)
        {
            var cell = table.Data[row].ColumnData.Span[column];
            Assert.True(cell.HasValue);
            return Encoding.UTF8.GetString(cell.Value.Span);
        }
    }
}
