using System;
using System.Collections.Generic;
using System.Reflection;
using Raven.Server.Integrations.PostgreSQL;
using Raven.Server.Integrations.PostgreSQL.PowerBI;
using Raven.Server.Integrations.PostgreSQL.Translation;
using Sparrow.Extensions;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL.PowerBI
{
    public sealed class PowerBIAstTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void Simple_public_table_fetch_should_translate_via_ast_and_strip_table_alias_and_json_projection()
        {
            const string sql = """
                select "$Table"."id()" as "id()", "$Table"."Company" as "Company", "$Table"."json()" as "json()"
                from "public"."Orders" "$Table" limit 200
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            var rqlQuery = Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(rqlQuery);

            Assert.NotNull(queryString);
            Assert.Contains("from 'Orders'", queryString);
            Assert.Contains("select", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("id()", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("$Table.", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("json()", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("limit 0, 200", queryString, StringComparison.OrdinalIgnoreCase);
        }

        // PowerBI's distinct-values probe (slicer / dropdown population): SELECT user_cols + GROUP BY
        // same columns. The user didn't ask for id() or json(), so the RowDescription must NOT
        // include them - PowerBI compares its requested column count to the schema and raises
        // `Field count mismatch when mapping column types. N vs M` if the server adds synthetic
        // columns the SQL never named. Pinned via dispatch type: PgSqlTranslatedRqlQuery has both
        // synthetic-column flags off; PowerBIRqlQuery has them on.
        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void Projected_fetch_without_id_or_json_routes_to_PgSqlTranslatedRqlQuery()
        {
            const string sql = """
                select "rows"."Company" as "Company", "rows"."Freight" as "Freight"
                from "public"."Orders" "rows"
                group by "Company", "Freight"
                limit 1000001
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PgSqlTranslatedRqlQuery>(pgQuery);
        }

        // Narrow projection that doesn't name id() or json() - the user asked for specific
        // user columns and expects the response to match column-for-column. Adding synthetic
        // id() and json() on top widens the RowDescription past the requested set and PowerBI's
        // mashup engine drops the connection. Row identity for DirectQuery comes from the PK
        // metadata declared in information_schema (table_constraints + key_column_usage), not
        // from the per-query projection.
        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void Narrow_projection_without_id_or_json_routes_to_PgSqlTranslatedRqlQuery()
        {
            const string sql = """
                select "Company", "Freight"
                from "public"."Orders"
                limit 100
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PgSqlTranslatedRqlQuery>(pgQuery);
        }

        // PowerBI's standard post-aggregate fetch shape: `select _.col, _.aggAlias from (group by/aggregate) _
        // where not _.aggAlias is null`. The OUTERMOST projection is narrow user columns + an aggregate
        // output alias, never id()/json() - so synthetic columns must not be appended or the RowDescription
        // widens by one and PowerBI errors out mid-query with
        // `Field count mismatch when mapping column types. 2 vs 3`. Pin via dispatch type.
        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void Wrapped_post_aggregate_narrow_projection_routes_to_PgSqlTranslatedRqlQuery()
        {
            const string sql = """
                select "_"."Freight",
                    "_"."a0"
                from
                (
                    select "rows"."Freight" as "Freight",
                        sum("rows"."Freight") as "a0"
                    from "public"."Orders" "rows"
                    group by "Freight"
                ) "_"
                where not "_"."a0" is null
                limit 1000001
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PgSqlTranslatedRqlQuery>(pgQuery);
        }

        // Narrow projection + const-marker (`1 as "c0"`) - PowerBI's row-preview shape
        // sometimes attaches a constant projection to a narrow column list. The presence of
        // the const marker must NOT force routing through PowerBIRqlQuery (which would also
        // add the unwanted id+json synthetic columns); PgSqlTranslatedRqlQuery now carries
        // the const-projection plumbing too, so narrow + const stays narrow.
        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void Narrow_projection_with_const_marker_routes_to_PgSqlTranslatedRqlQuery()
        {
            const string sql = """
                select "_"."Company" as "Company", 1 as "c0"
                from (from Orders) "_"
                limit 100
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PgSqlTranslatedRqlQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_match_wrapped_rql_fetch_shape_and_apply_outer_limit()
        {
            const string sql = """select * from (from Employees) "$Table" limit 1000""";

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            Assert.Contains("from Employees", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));
        }

        // --- Item B: wrapper interpretation tolerance ---

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void Fetch_rows_alias_at_wrapper_level_should_be_accepted()
        {
            // Sub-item 1 of Item B: the "rows" wrapper alias variant must be accepted by the Fetch
            // walker just like "_" and "$Table". Pinned here so future narrowing of the alias list
            // (e.g. accidentally returning to the old "_/$Table" only check) regresses loudly.
            const string sql = """select * from (from Employees) "rows" limit 1000""";

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            Assert.Equal(1000, GetLimit(pgQuery));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_aggregate_with_offset_zero_should_be_accepted()
        {
            // Item B relax #1: LIMIT N OFFSET 0 is a semantic no-op at the outer aggregate level.
            // PowerBI occasionally emits it; DirectQuery must accept it instead of bailing out.
            const string sql = """
                select "_"."Employee",
                    "_"."a0"
                from
                (
                    select "rows"."Employee" as "Employee",
                        sum("rows"."Freight") as "a0"
                    from
                    (
                        from Orders
                    ) "rows"
                    group by "Employee"
                ) "_"
                limit 1000 offset 0
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_grouped_aggregate_with_spaced_field_names_should_be_accepted()
        {
            // Field/alias names with spaces have no break-out characters and are quoted by the AST
            // visitor that renders the grouped RQL, so the shape is accepted, not dropped by a guard.
            const string sql = """
                select "_"."Unit Price",
                    "_"."a0"
                from
                (
                    select "rows"."Unit Price" as "Unit Price",
                        sum("rows"."Net Amount") as "a0"
                    from
                    (
                        from Orders
                    ) "rows"
                    group by "Unit Price"
                ) "_"
                limit 1000
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_group_by_with_typecast_should_be_accepted()
        {
            // Item B relax #2: GROUP BY through TypeCast/RelabelType must unwrap to the underlying
            // ColumnRef. PowerBI sometimes emits explicit type coercions in GROUP BY; the cast is
            // semantically irrelevant for RQL grouping.
            const string sql = """
                select "_"."Employee",
                    "_"."a0"
                from
                (
                    select "rows"."Employee" as "Employee",
                        sum("rows"."Freight") as "a0"
                    from
                    (
                        from Orders
                    ) "rows"
                    group by "Employee"::text
                ) "_"
                limit 1000
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_aggregate_with_non_underscore_order_by_alias_should_be_accepted()
        {
            // Item B relax #4: ORDER BY on a non-"_" wrapper alias (e.g. "rows"."a0") must be
            // tolerated. The qualifier is a wrapper alias; only the last identifier segment is
            // meaningful for RQL. Also exercises relax #3 (multi-segment projection via "rows".X).
            const string sql = """
                select "rows"."Employee" as "Employee",
                    "rows"."a0" as "a0"
                from
                (
                    select "$Table"."Employee" as "Employee",
                        sum("$Table"."Freight") as "a0"
                    from
                    (
                        from Orders
                    ) "$Table"
                    group by "Employee"
                ) "rows"
                order by "rows"."a0"
                limit 1000
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_desktop_null_order_helper_wrapper_should_be_handled_by_direct_query_parser_not_fetch()
        {
            const string sql = """select "_"."OrderedAt" as "c2", "_"."RequireAt" as "c3" from ( select "OrderedAt", "RequireAt", "_"."t0_0" as "t0_0", "_"."t1_0" as "t1_0", "_"."t2_0" as "t2_0", "_"."t3_0" as "t3_0" from ( select "_"."OrderedAt", "_"."RequireAt", "_"."o0", "_"."o1", "_"."t0_0", "_"."t1_0", "_"."t2_0", "_"."t3_0" from ( select "_"."OrderedAt" as "OrderedAt", "_"."RequireAt" as "RequireAt", "_"."o0" as "o0", "_"."o1" as "o1", case when "_"."o0" is not null then "_"."o0" else timestamp '1899-12-28 00:00:00' end as "t0_0", case when "_"."o0" is null then 0 else 1 end as "t1_0", case when "_"."o1" is not null then "_"."o1" else timestamp '1899-12-28 00:00:00' end as "t2_0", case when "_"."o1" is null then 0 else 1 end as "t3_0" from ( select "rows"."OrderedAt" as "OrderedAt", "rows"."RequireAt" as "RequireAt", "rows"."o0" as "o0", "rows"."o1" as "o1" from ( select "OrderedAt" as "OrderedAt", "RequireAt" as "RequireAt", "OrderedAt" as "o0", "RequireAt" as "o1" from ( from Orders as o where o.Company = "Companies/1-A" OR o.Company = "Companies/2-A" select { OrderedAt: o.OrderedAt, RequireAt: o.RequireAt} ) "$Table" ) "rows" group by "OrderedAt", "RequireAt", "o0", "o1" ) "_" ) "_" ) "_" ) "_" order by "_"."t0_0", "_"."t1_0", "_"."t2_0", "_"."t3_0" limit 501""";

            // Use the same PowerBI parsing entry point as production.
            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            // 4-level null-order-helper wrapper with inner RQL - classified as DirectQuery (non-aggregate path).
            Assert.IsType<PowerBIDirectQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_grouped_aggregate_with_non_group_key_pre_filter_falls_through()
        {
            // sum(Freight) grouped by Employee+RequireAt, pre-filtered on Company (not a group key). A
            // grouped RQL WHERE is post-reduction (HAVING) and can't express a pre-group filter on a
            // non-key field, so recognition falls through to the diagnoser instead of emitting RQL the
            // engine rejects at execution.
            const string sql = """
                select "_"."Employee",
                    "_"."RequireAt",
                    "_"."a0"
                from
                (
                    select "rows"."Employee" as "Employee",
                        "rows"."RequireAt" as "RequireAt",
                        sum("rows"."Freight") as "a0"
                    from
                    (
                        from Orders
                        where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
                    ) "rows"
                    group by "Employee",
                        "RequireAt"
                ) "_"
                where not "_"."a0" is null
                limit 1000001
                """;

            Assert.False(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi, Skip = "Scalar aggregates are intentionally unsupported (RQL requires group by for sum). Remove/enable when scalar support is implemented.")]
        public void DirectQuery_aggregate_only_sum_should_be_classified_as_direct_query()
        {
            const string sql = """
                select sum("rows"."Freight") as "a0"
                from
                (
                    select "Freight"
                    from
                    (
                        from Orders
                    ) "$Table"
                ) "rows"
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_desktop_grouped_sum_two_group_fields_with_inner_filter_on_group_field_should_be_classified_as_direct_query()
        {
            const string sql = """
                select "_"."Employee",
                    "_"."RequireAt",
                    "_"."a0"
                from
                (
                    select "rows"."Employee" as "Employee",
                        "rows"."RequireAt" as "RequireAt",
                        sum("rows"."Freight") as "a0"
                    from
                    (
                        from Orders
                        where Employee != null
                    ) "rows"
                    group by "Employee",
                        "RequireAt"
                ) "_"
                where not "_"."a0" is null
                limit 1000001
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_desktop_employee_requireAt_json_with_null_order_helper_columns_should_be_classified_as_direct_query()
        {
            const string sql = """
                select "_"."Employee" as "c3",
                    "_"."RequireAt" as "c7",
                    "_"."json()" as "c11"
                from
                (
                    select "Employee",
                        "RequireAt",
                        "json()",
                        "_"."t2_0" as "t2_0",
                        "_"."t3_0" as "t3_0"
                    from
                    (
                        select "_"."Employee",
                            "_"."RequireAt",
                            "_"."json()",
                            "_"."o2",
                            "_"."t2_0",
                            "_"."t3_0"
                        from
                        (
                            select "_"."Employee" as "Employee",
                                "_"."RequireAt" as "RequireAt",
                                "_"."json()" as "json()",
                                "_"."o2" as "o2",
                                case
                                    when "_"."o2" is not null
                                    then "_"."o2"
                                    else timestamp '1899-12-28 00:00:00'
                                end as "t2_0",
                                case
                                    when "_"."o2" is null
                                    then 0
                                    else 1
                                end as "t3_0"
                            from
                            (
                                select "rows"."Employee" as "Employee",
                                    "rows"."RequireAt" as "RequireAt",
                                    "rows"."json()" as "json()",
                                    "rows"."o2" as "o2"
                                from
                                (
                                    select "Employee" as "Employee",
                                        "RequireAt" as "RequireAt",
                                        "json()" as "json()",
                                        "RequireAt" as "o2"
                                    from
                                    (
                                        from Orders
                                        where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
                                    ) "$Table"
                                ) "rows"
                                group by "Employee",
                                    "RequireAt",
                                    "json()",
                                    "o2"
                            ) "_"
                        ) "_"
                    ) "_"
                ) "_"
                order by "_"."Employee",
                        "_"."json()",
                        "_"."t2_0",
                        "_"."t3_0"
                limit 501
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_distinct_list_wrapper_single_column_orders_employee_should_be_classified_as_direct_query()
        {
            const string sql = """
                select "_"."Employee"
                from
                (
                    select "rows"."Employee" as "Employee"
                    from
                    (
                        select "Employee"
                        from
                        (
                            from Orders
                            where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
                        ) "$Table"
                    ) "rows"
                    group by "Employee"
                ) "_"
                order by "_"."Employee"
                limit 1001
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_distinct_list_wrapper_with_inner_rql_load_should_report_actual_parser_classification()
        {
            const string sql = """
                select "_"."Name",
                        "_"."Manager"
                from
                (
                    select "rows"."Name" as "Name",
                        "rows"."Manager" as "Manager"
                    from
                    (
                        select "Name",
                            "Manager"
                        from
                        (
                            from Employees as e
                            where id() in ('employees/1-A')
                            load e.ReportsTo as boss
                            select { Name: e.FirstName, Manager: boss.FirstName }
                        ) "$Table"
                    ) "rows"
                    group by "Name",
                        "Manager"
                ) "_"
                order by "_"."Name",
                        "_"."Manager"
                limit 501
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIDirectQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_desktop_grouped_sum_wrapper_should_be_handled_by_direct_query_parser_not_fetch()
        {
            const string sql = """
                select "_"."Company" as "c2",
                    "_"."a0" as "a0"
                from
                (
                    select "_"."Company",
                        "_"."a0"
                    from
                    (
                        select "_"."Company",
                            "_"."a0"
                        from
                        (
                            select "rows"."Company" as "Company",
                                sum("rows"."Freight") as "a0"
                            from
                            (
                                select "Company",
                                    "Freight"
                                from
                                (
                                    from Orders as o
                                    where o.Company = "Companies/1-A" OR o.Company = "Companies/2-A"
                                    select { Company: o.Company, Freight: o.Freight}
                                ) "$Table"
                            ) "rows"
                            group by "Company"
                        ) "_"
                        where not "_"."a0" is null
                    ) "_"
                ) "_"
                order by "_"."a0" desc,
                        "_"."Company"
                limit 1001
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_match_wrapped_rql_fetch_with_outer_where_not_equal_or_is_null()
        {
            const string sql = """
                select "_"."id()" as "id()", "_"."FirstName" as "FirstName"
                from
                (
                    from Employees
                ) "_"
                where ("_"."FirstName" <> 'Anne' or "_"."FirstName" is null)
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(pgQuery);
            Assert.Contains("from Employees", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("FirstName", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Anne", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("or", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("null", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_translate_between_in_outer_where()
        {
            const string sql = """
                select "_"."id()" as "id()", "_"."OrderedAt" as "OrderedAt"
                from
                (
                    from Orders
                ) "_"
                where "_"."OrderedAt" between 1 and 10
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(pgQuery);
            Assert.Contains("from Orders", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("OrderedAt", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(">= 1", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("<= 10", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_merge_inner_where_with_outer_where()
        {
            const string sql = """
                select "_"."id()" as "id()", "_"."FirstName" as "FirstName"
                from
                (
                    from Employees where startsWith(LastName, 'D')
                ) "_"
                where "_"."FirstName" = 'Anne'
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(pgQuery);
            Assert.Contains("from Employees", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("startsWith", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("FirstName", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Anne", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_translate_timestamp_literal_in_outer_where()
        {
            const string sql = """
                select "_"."id()" as "id()", "_"."HiredAt" as "HiredAt"
                from
                (
                    from Employees
                ) "_"
                where "_"."HiredAt" = timestamp '1994-11-15 00:00:00' and "_"."HiredAt" is not null
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(pgQuery);
            Assert.Contains("from Employees", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("HiredAt", queryString, StringComparison.OrdinalIgnoreCase);

            var expected = new DateTime(1994, 11, 15, 0, 0, 0).GetDefaultRavenFormat();
            Assert.Contains(expected, queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_extract_single_replace_projection_via_ast()
        {
            const string sql = """
                select "_"."id()" as "id()", "_"."Title" as "Title", replace("_"."Title", 'Sales', 'Marketing') as "t0_0"
                from
                (
                    from Employees where startsWith(LastName, 'D') select Title
                ) "_"
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            Assert.Contains("from Employees", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("startsWith", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));

            var replaces = GetReplaces(pgQuery);
            Assert.NotNull(replaces);
            Assert.True(replaces.TryGetValue("Title", out var r));
            Assert.Equal("t0_0", r.DstColumnName);
            Assert.Equal("Title", r.SrcColumnName);
            Assert.Equal("Sales", r.OldValue);
            Assert.Equal("Marketing", r.NewValue);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_extract_multiple_replace_projections_via_ast()
        {
            const string sql = """
                select "_"."id()" as "id()",
                    "_"."Title" as "Title",
                    replace("_"."Title", 'Sales', 'Marketing') as "t0_0",
                    replace("_"."FirstName", 'Anne', 'Annie') as "t0_1"
                from
                (
                    from Employees where startsWith(LastName, 'D') select Title, FirstName
                ) "_"
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            Assert.Contains("from Employees", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("startsWith", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));

            var replaces = GetReplaces(pgQuery);
            Assert.NotNull(replaces);

            Assert.True(replaces.TryGetValue("Title", out var titleReplace));
            Assert.Equal("t0_0", titleReplace.DstColumnName);
            Assert.Equal("Title", titleReplace.SrcColumnName);
            Assert.Equal("Sales", titleReplace.OldValue);
            Assert.Equal("Marketing", titleReplace.NewValue);

            Assert.True(replaces.TryGetValue("FirstName", out var firstNameReplace));
            Assert.Equal("t0_1", firstNameReplace.DstColumnName);
            Assert.Equal("FirstName", firstNameReplace.SrcColumnName);
            Assert.Equal("Anne", firstNameReplace.OldValue);
            Assert.Equal("Annie", firstNameReplace.NewValue);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_support_multi_nested_wrapped_rql_with_multiple_wheres_and_replace()
        {
            const string sql = """
                select "_"."id()" as "id()", replace("_"."Title", 'Sales', 'Marketing') as "t0_0"
                from
                (
                    select "_"."id()" as "id()", "_"."Title" as "Title"
                    from
                    (
                        from Employees where startsWith(LastName, 'D')
                    ) "_"
                    where "_"."FirstName" = 'Anne'
                ) "_"
                where (("_"."LastName" <> 'Dodsworth' or "_"."LastName" is null) and "_"."ReportsTo" between 1 and 10)
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(pgQuery);
            Assert.Contains("from Employees", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("startsWith", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Anne", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Dodsworth", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(">= 1", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("<= 10", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));

            var replaces = GetReplaces(pgQuery);
            Assert.NotNull(replaces);
            Assert.True(replaces.TryGetValue("Title", out var r));
            Assert.Equal("t0_0", r.DstColumnName);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_support_deep_wrapper_nesting_with_complex_outer_where_and_multiple_replace_levels_and_huge_inner_rql()
        {
            var innerRql = """
                from 'Users' as u
                where u.Age > 0 and u.Name != null
                order by u.Name
                select {
                    Name: u.Name,
                    Age: u.Age,
                    Title: u.Title,
                    Field01: u.Field01,
                    Field02: u.Field02,
                    Field03: u.Field03,
                    Field04: u.Field04,
                    Field05: u.Field05,
                    Field06: u.Field06,
                    Field07: u.Field07,
                    Field08: u.Field08,
                    Field09: u.Field09,
                    Field10: u.Field10,
                    VeryLongTailMarker123: u.VeryLongTailMarker123
                }
                """;

            var sql = $$"""
                select "_"."id()",
                       "_"."Company",
                       "_"."Employee",
                       "_"."Freight",
                       "_"."Lines",
                       "_"."OrderedAt",
                       "_"."RequireAt",
                       "_"."ShipTo",
                       "_"."ShipVia",
                       "_"."ShippedAt",
                       "_"."json()"
                from
                (
                    select "_"."id()" as "id()",
                           "_"."Company" as "Company",
                           "_"."Employee" as "Employee",
                           "_"."Freight" as "Freight",
                           "_"."Lines" as "Lines",
                           "_"."OrderedAt" as "OrderedAt",
                           "_"."RequireAt" as "RequireAt",
                           "_"."ShipTo" as "ShipTo",
                           "_"."ShipVia" as "ShipVia",
                           "_"."ShippedAt" as "ShippedAt",
                           "_"."json()" as "json()",
                           "_"."FirstName" as "FirstName",
                           replace("_"."FirstName", 'Ann', 'Anne') as "t1_0"
                    from
                    (
                        select "$Table"."id()" as "id()",
                               "$Table"."Company" as "Company",
                               "$Table"."Employee" as "Employee",
                               "$Table"."Freight" as "Freight",
                               "$Table"."Lines" as "Lines",
                               "$Table"."OrderedAt" as "OrderedAt",
                               "$Table"."RequireAt" as "RequireAt",
                               "$Table"."ShipTo" as "ShipTo",
                               "$Table"."ShipVia" as "ShipVia",
                               "$Table"."ShippedAt" as "ShippedAt",
                               "$Table"."json()" as "json()",
                               "$Table"."Title" as "Title",
                               replace("$Table"."Title", 'Sales', 'Marketing') as "t0_0"
                        from
                        (
                            select "_"."id()" as "id()",
                                   "_"."Age" as "Age",
                                   "_"."Name" as "Name",
                                   "_"."Company" as "Company",
                                   "_"."Employee" as "Employee",
                                   "_"."Freight" as "Freight",
                                   "_"."Lines" as "Lines",
                                   "_"."OrderedAt" as "OrderedAt",
                                   "_"."RequireAt" as "RequireAt",
                                   "_"."ShipTo" as "ShipTo",
                                   "_"."ShipVia" as "ShipVia",
                                   "_"."ShippedAt" as "ShippedAt",
                                   "_"."json()" as "json()",
                                   "_"."Title" as "Title",
                                   "_"."DeletedAt" as "DeletedAt",
                                   "_"."IsActive" as "IsActive",
                                   "_"."Score" as "Score",
                                   "_"."FirstName" as "FirstName"
                            from
                            (
                                {{innerRql}}
                            ) "_"
                            where (("_"."Age" between 10 and 20 and "_"."Name" in ('a','b','c')) and ("_"."DeletedAt" is null))
                        ) "$Table"
                        where (((not ("$Table"."IsActive" = true)) or ("$Table"."Score" <> 5)) and "$Table"."Title" is not null)
                    ) "_"
                    where ("_"."FirstName" is not null and "_"."Score" <> 5)
                ) "_"
                where (("_"."Company" is not null) and (("_"."Freight" <> 5) or ("_"."Freight" is null)) and ("_"."OrderedAt" between 1 and 10))
                limit 25
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);

            var expected = """
FROM Users AS u WHERE (((((u.Age > 0 AND u.Name != null) AND ((u.Company != null AND (u.Freight != 5 OR u.Freight = null)) AND (u.OrderedAt >= 1 AND u.OrderedAt <= 10))) AND (u.FirstName != null AND u.Score != 5)) AND ((u.IsActive != true OR u.Score != 5) AND u.Title != null)) AND (((u.Age >= 10 AND u.Age <= 20) AND u.Name IN ('a', 'b', 'c')) AND u.DeletedAt = null))
ORDER BY u.Name
SELECT { 

    Name: u.Name,
    Age: u.Age,
    Title: u.Title,
    Field01: u.Field01,
    Field02: u.Field02,
    Field03: u.Field03,
    Field04: u.Field04,
    Field05: u.Field05,
    Field06: u.Field06,
    Field07: u.Field07,
    Field08: u.Field08,
    Field09: u.Field09,
    Field10: u.Field10,
    VeryLongTailMarker123: u.VeryLongTailMarker123

}
""";
            Assert.Equal(Normalize(expected), Normalize(queryString));

            Assert.Equal(25, GetLimit(pgQuery));

            var replaces = GetReplaces(pgQuery);
            Assert.NotNull(replaces);

            Assert.True(replaces.TryGetValue("Title", out var titleReplace));
            Assert.Equal("t0_0", titleReplace.DstColumnName);
            Assert.Equal("Title", titleReplace.SrcColumnName);
            Assert.Equal("Sales", titleReplace.OldValue);
            Assert.Equal("Marketing", titleReplace.NewValue);

            Assert.True(replaces.TryGetValue("FirstName", out var firstNameReplace));
            Assert.Equal("t1_0", firstNameReplace.DstColumnName);
            Assert.Equal("FirstName", firstNameReplace.SrcColumnName);
            Assert.Equal("Ann", firstNameReplace.OldValue);
            Assert.Equal("Anne", firstNameReplace.NewValue);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_support_declare_function_inner_rql_with_wrappers_outer_where_and_limit()
        {
            var innerRql = """
                declare function isGood(u) { return u.Age > 18; }
                from 'Users' as u
                where isGood(u)
                select { Name: u.Name, Age: u.Age }
                """;

            var sql = $$"""
                select "_"."id()",
                       "_"."Company",
                       "_"."Employee",
                       "_"."Freight",
                       "_"."Lines",
                       "_"."OrderedAt",
                       "_"."RequireAt",
                       "_"."ShipTo",
                       "_"."ShipVia",
                       "_"."ShippedAt",
                       "_"."json()"
                from
                (
                    select "$Table"."id()" as "id()",
                           "$Table"."Company" as "Company",
                           "$Table"."Employee" as "Employee",
                           "$Table"."Freight" as "Freight",
                           "$Table"."Lines" as "Lines",
                           "$Table"."OrderedAt" as "OrderedAt",
                           "$Table"."RequireAt" as "RequireAt",
                           "$Table"."ShipTo" as "ShipTo",
                           "$Table"."ShipVia" as "ShipVia",
                           "$Table"."ShippedAt" as "ShippedAt",
                           "$Table"."json()" as "json()",
                           "$Table"."Name" as "Name",
                           replace("$Table"."Name", 'a', 'b') as "n0"
                    from
                    (
                        select "_"."Name" as "Name"
                        from
                        (
                            {{innerRql}}
                        ) "_"
                    ) "$Table"
                    where ("$Table"."Name" in ('a','b') and "$Table"."Name" is not null)
                ) "_"
                where ("_"."Company" in ('a','b') and "_"."Company" is not null)
                limit 7
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);

            var expected = """
DECLARE function isGood(u) { return u.Age > 18; }

FROM Users AS u WHERE ((isGood(u) AND (u.Company IN ('a', 'b') AND u.Company != null)) AND (u.Name IN ('a', 'b') AND u.Name != null))
SELECT { 
 Name: u.Name, Age: u.Age 
}
""";
            Assert.Equal(Normalize(expected), Normalize(queryString));
            Assert.Equal(7, GetLimit(pgQuery));

            var replaces = GetReplaces(pgQuery);
            Assert.NotNull(replaces);
            Assert.True(replaces.TryGetValue("Name", out var nameReplace));
            Assert.Equal("n0", nameReplace.DstColumnName);
            Assert.Equal("Name", nameReplace.SrcColumnName);
            Assert.Equal("a", nameReplace.OldValue);
            Assert.Equal("b", nameReplace.NewValue);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_support_projection_heavy_multi_wrapper_with_replace_at_outer_and_inner_levels_and_complex_outer_where()
        {
            var innerRql = """
                from Orders as o
                where o.Company != null
                select { Company: o.Company, Employee: o.Employee, Freight: o.Freight, OrderedAt: o.OrderedAt, ShippedAt: o.ShippedAt }
                """;

            const int limit = 33;

            var sql = $$"""
                select "_"."id()",
                       "_"."Company",
                       "_"."Employee",
                       replace("_"."Employee", 'X', 'Y') as "tOuter_0",
                       "_"."Freight",
                       "_"."Lines",
                       "_"."OrderedAt",
                       "_"."RequireAt",
                       "_"."ShipTo",
                       "_"."ShipVia",
                       "_"."ShippedAt",
                       "_"."json()"
                from
                (
                    select "_"."id()" as "id()",
                           "_"."Company" as "Company",
                           "_"."Employee" as "Employee",
                           "_"."Freight" as "Freight",
                           "_"."Lines" as "Lines",
                           "_"."OrderedAt" as "OrderedAt",
                           "_"."RequireAt" as "RequireAt",
                           "_"."ShipTo" as "ShipTo",
                           "_"."ShipVia" as "ShipVia",
                           "_"."ShippedAt" as "ShippedAt",
                           "_"."json()" as "json()"
                    from
                    (
                        select "$Table"."id()" as "id()",
                               "$Table"."Company" as "Company",
                               replace("$Table"."Company", 'A', 'B') as "tInner_0",
                               "$Table"."Employee" as "Employee",
                               "$Table"."Freight" as "Freight",
                               "$Table"."Lines" as "Lines",
                               "$Table"."OrderedAt" as "OrderedAt",
                               "$Table"."RequireAt" as "RequireAt",
                               "$Table"."ShipTo" as "ShipTo",
                               "$Table"."ShipVia" as "ShipVia",
                               "$Table"."ShippedAt" as "ShippedAt",
                               "$Table"."json()" as "json()"
                        from
                        (
                            {{innerRql}}
                        ) "$Table"
                    ) "_"
                ) "_"
                where ((("_"."Employee" <> 'X' or "_"."Employee" is null) and ("_"."ShippedAt" is not null)) and ("_"."Freight" between 10 and 20))
                limit {{limit}}
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);

            var expected = """
FROM Orders AS o WHERE (o.Company != null AND (((o.Employee != 'X' OR o.Employee = null) AND o.ShippedAt != null) AND (o.Freight >= 10 AND o.Freight <= 20)))
SELECT { 
 Company: o.Company, Employee: o.Employee, Freight: o.Freight, OrderedAt: o.OrderedAt, ShippedAt: o.ShippedAt 
}
""";
            Assert.Equal(Normalize(expected), Normalize(queryString));
            Assert.Equal(limit, GetLimit(pgQuery));

            var replaces = GetReplaces(pgQuery);
            Assert.NotNull(replaces);

            Assert.True(replaces.TryGetValue("Employee", out var employeeReplace));
            Assert.Equal("tOuter_0", employeeReplace.DstColumnName);
            Assert.Equal("Employee", employeeReplace.SrcColumnName);
            Assert.Equal("X", employeeReplace.OldValue);
            Assert.Equal("Y", employeeReplace.NewValue);

            Assert.True(replaces.TryGetValue("Company", out var companyReplace));
            Assert.Equal("tInner_0", companyReplace.DstColumnName);
            Assert.Equal("Company", companyReplace.SrcColumnName);
            Assert.Equal("A", companyReplace.OldValue);
            Assert.Equal("B", companyReplace.NewValue);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_support_projection_heavy_wrapper_with_large_inner_rql_and_in_and_not_outer_where_and_limit()
        {
            var innerRql = """
                from 'Users' as u
                where u.Age > 0
                order by u.Name
                select {
                    Name: u.Name,
                    Age: u.Age,
                    Company: u.Company,
                    Employee: u.Employee,
                    Freight: u.Freight,
                    Lines: u.Lines,
                    OrderedAt: u.OrderedAt,
                    RequireAt: u.RequireAt,
                    ShipTo: u.ShipTo,
                    ShipVia: u.ShipVia,
                    ShippedAt: u.ShippedAt,
                    TailMarkerB987: u.TailMarkerB987
                }
                """;

            const int limit = 12;

            var sql = $$"""
                select "_"."id()",
                       "_"."Company",
                       "_"."Employee",
                       "_"."Freight",
                       "_"."Lines",
                       "_"."OrderedAt",
                       "_"."RequireAt",
                       "_"."ShipTo",
                       "_"."ShipVia",
                       "_"."ShippedAt",
                       "_"."json()"
                from
                (
                    select "_"."id()" as "id()",
                           "_"."Company" as "Company",
                           "_"."Employee" as "Employee",
                           "_"."Freight" as "Freight",
                           "_"."Lines" as "Lines",
                           "_"."OrderedAt" as "OrderedAt",
                           "_"."RequireAt" as "RequireAt",
                           "_"."ShipTo" as "ShipTo",
                           "_"."ShipVia" as "ShipVia",
                           "_"."ShippedAt" as "ShippedAt",
                           "_"."json()" as "json()"
                    from
                    (
                        {{innerRql}}
                    ) "_"
                ) "_"
                where (("_"."Company" in ('a','b','c')) and (not ("_"."Company" in ('x','y'))))
                limit {{limit}}
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);

            var expected = """
FROM Users AS u WHERE (u.Age > 0 AND (u.Company IN ('a', 'b', 'c') AND NOT (u.Company IN ('x', 'y'))))
ORDER BY u.Name
SELECT { 

    Name: u.Name,
    Age: u.Age,
    Company: u.Company,
    Employee: u.Employee,
    Freight: u.Freight,
    Lines: u.Lines,
    OrderedAt: u.OrderedAt,
    RequireAt: u.RequireAt,
    ShipTo: u.ShipTo,
    ShipVia: u.ShipVia,
    ShippedAt: u.ShippedAt,
    TailMarkerB987: u.TailMarkerB987

}
""";
            Assert.Equal(Normalize(expected), Normalize(queryString));
            Assert.Equal(limit, GetLimit(pgQuery));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_extract_nested_replace_projection_aliases_as_keys()
        {
            const string sql = """
                select "_"."id()" as "id()",
                    "_"."LastName" as "LastName",
                    "_"."FirstName" as "FirstName",
                    "_"."json()" as "json()",
                    "_"."t0_0" as "t0_0",
                    "_"."t0_03" as "t0_03",
                    replace("_"."t0_0", 'aaa', 'bbb') as "t0_02",
                    replace("_"."t0_03", 'Steven', 'ddd') as "t0_04"
                from
                (
                    select "_"."id()" as "id()",
                        "_"."LastName" as "LastName",
                        "_"."FirstName" as "FirstName",
                        "_"."json()" as "json()",
                        replace("_"."LastName", 'Dodsworth', 'aaa') as "t0_0",
                        replace("_"."FirstName", 'Janet', 'ccc') as "t0_03"
                    from
                    (
                        from Employees
                    ) "_"
                ) "_"
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            Assert.Equal(1000, GetLimit(pgQuery));

            var replaces = GetReplaces(pgQuery);
            Assert.NotNull(replaces);

            Assert.True(replaces.TryGetValue("LastName", out var r0));
            Assert.Equal("t0_0", r0.DstColumnName);
            Assert.Equal("LastName", r0.SrcColumnName);
            Assert.Equal("Dodsworth", r0.OldValue);
            Assert.Equal("aaa", r0.NewValue);

            Assert.True(replaces.TryGetValue("FirstName", out var r03));
            Assert.Equal("t0_03", r03.DstColumnName);
            Assert.Equal("FirstName", r03.SrcColumnName);
            Assert.Equal("Janet", r03.OldValue);
            Assert.Equal("ccc", r03.NewValue);

            Assert.True(replaces.TryGetValue("t0_0", out var r02));
            Assert.Equal("t0_02", r02.DstColumnName);
            Assert.Equal("t0_0", r02.SrcColumnName);
            Assert.Equal("aaa", r02.OldValue);
            Assert.Equal("bbb", r02.NewValue);

            Assert.True(replaces.TryGetValue("t0_03", out var r04));
            Assert.Equal("t0_04", r04.DstColumnName);
            Assert.Equal("t0_03", r04.SrcColumnName);
            Assert.Equal("Steven", r04.OldValue);
            Assert.Equal("ddd", r04.NewValue);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_match_wrapped_rql_fetch_shape_with_outer_where_and_apply_filter_to_inner_alias()
        {
            const string sql = """
                select "_"."id()",
                    "_"."FirstName",
                    "_"."json()"
                from
                (
                    from Employees as e
                ) "_"
                where "_"."FirstName" = 'Anne' and "_"."FirstName" is not null
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            Assert.Contains("from Employees", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("where", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("FirstName", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Anne", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_match_wrapped_rql_fetch_with_outer_where_swapped_predicate_order()
        {
            const string sql = """
                select "_"."id()",
                    "_"."FirstName",
                    "_"."json()"
                from
                (
                    from Employees as e
                ) "_"
                where "_"."FirstName" is not null and "_"."FirstName" = 'Anne'
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            Assert.Contains("from Employees", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("where", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("FirstName", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Anne", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_match_wrapped_rql_fetch_with_outer_where_or_nesting()
        {
            const string sql = """
                select "_"."id()",
                    "_"."FirstName",
                    "_"."Title"
                from
                (
                    from Employees as e
                ) "_"
                where (("_"."FirstName" <> 'Anne' or "_"."FirstName" is null) and ("_"."Title" = 'Vice President, Sales' and "_"."Title" is not null))
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);

            var queryString = GetQueryString(pgQuery);
            Assert.Contains("from Employees", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("where", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("FirstName", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Anne", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Title", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Vice President, Sales", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_match_wrapped_rql_fetch_shape_with_underscore_alias_and_limit_0()
        {
            const string sql = """select * from (from Orders) "_" limit 0""";

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            Assert.Contains("from Orders", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(0, GetLimit(pgQuery));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void TryParse_should_match_projected_wrapped_rql_fetch_shape_and_apply_outer_limit()
        {
            const string sql = """
                select "$Table"."id()" as "id()",
                       "$Table"."LastName" as "LastName",
                       "$Table"."FirstName" as "FirstName",
                       "$Table"."json()" as "json()"
                from
                (
                    from Employees
                ) "$Table"
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            Assert.IsType<PowerBIRqlQuery>(pgQuery);
            Assert.Contains("from Employees", GetQueryString(pgQuery), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1000, GetLimit(pgQuery));
        }

        // PowerBI incremental refresh
        //
        // Incremental refresh fires repeated queries with a half-open date window
        // (`WHERE "col" >= start AND "col" < end`) once per refresh partition, wrapping the
        // table as `"public"."Coll" "$Table"`. These tests cover both the inline `timestamp 'X'`
        // literal and the parameterized ($1/$2) form.

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void IncrementalRefresh_WrappedTableWithDateRange_TranslatesToRangeFilter()
        {
            // Shape PowerBI emits for the simplest incremental refresh: explicit projection
            // of all columns it cares about, wrapped table reference, half-open date range.
            const string sql = """
                select "$Table"."id()" as "id()", "$Table"."OrderedAt" as "OrderedAt", "$Table"."Freight" as "Freight"
                from "public"."Orders" "$Table"
                where "$Table"."OrderedAt" >= timestamp '2024-01-01' and "$Table"."OrderedAt" < timestamp '2024-02-01'
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            var rqlQuery = Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var rql = GetQueryString(rqlQuery);

            Assert.NotNull(rql);
            Assert.Contains("from 'Orders'", rql, StringComparison.Ordinal);
            Assert.Contains("OrderedAt", rql, StringComparison.Ordinal);
            // Both range ends must survive translation - the whole point of incremental
            // refresh is that each partition's bounds reach RavenDB's index.
            Assert.Contains("2024-01", rql, StringComparison.Ordinal);
            Assert.Contains("2024-02", rql, StringComparison.Ordinal);
            // LIMIT is inlined into the emitted RQL by the simple-table-fetch path (rather
            // than stored on the PgQuery instance via _limit) - check the RQL text directly.
            Assert.Contains("1000", rql, StringComparison.Ordinal);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void IncrementalRefresh_WrappedTableWithDateRange_PreservesOrderByForPartitioning()
        {
            // Power Query sometimes adds an ORDER BY to make refresh deterministic. This
            // pins that adding a sort doesn't knock the query off the wrapped-fetch path.
            const string sql = """
                select "$Table"."id()" as "id()", "$Table"."OrderedAt" as "OrderedAt"
                from "public"."Orders" "$Table"
                where "$Table"."OrderedAt" >= timestamp '2024-01-01' and "$Table"."OrderedAt" < timestamp '2024-02-01'
                order by "$Table"."OrderedAt"
                limit 5000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            var rql = GetQueryString(pgQuery);
            Assert.NotNull(rql);
            Assert.Contains("from 'Orders'", rql, StringComparison.Ordinal);
            Assert.Contains("order by", rql, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("OrderedAt", rql, StringComparison.Ordinal);
        }

        // Parameterized form is what PowerBI Desktop actually wires up for incremental
        // refresh (RangeStart/RangeEnd bound at Bind time). The $N placeholders can't be
        // inlined at translate time, so the wrapped-fetch path emits RQL parameter references
        // ($1/$2) that PgQuery.Bind fills in from the OID-typed values at execute time.
        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void IncrementalRefresh_WrappedTableWithParameterizedDateRange_IsAccepted()
        {
            const string sql = """
                select "$Table"."id()" as "id()", "$Table"."OrderedAt" as "OrderedAt"
                from "public"."Orders" "$Table"
                where "$Table"."OrderedAt" >= $1 and "$Table"."OrderedAt" < $2
                limit 1000
                """;

            Assert.True(PowerBIFetchQuery.TryParse(sql, new int[] { 1114, 1114 }, documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIRqlQuery>(pgQuery);

            var rql = GetQueryString(pgQuery);
            Assert.NotNull(rql);
            Assert.Contains("from 'Orders'", rql, StringComparison.Ordinal);
            Assert.Contains("OrderedAt", rql, StringComparison.Ordinal);
            // 1-based PG index maps straight through to the RQL parameter name.
            Assert.Contains("$1", rql, StringComparison.Ordinal);
            Assert.Contains("$2", rql, StringComparison.Ordinal);
        }

        // PowerBI Desktop in DirectQuery mode plants user filters several wrapper levels deep -
        // here `where "_"."Freight" > 50` wraps the innermost projection, not the outermost SELECT.
        // The recognizer must pull it up; this test asserts the filter reaches the emitted RQL.
        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_with_intermediate_wrapper_level_where_should_apply_filter_to_inner_query()
        {
            const string sql = """
                select "_"."Freight" as "c32"
                from
                (
                    select "Freight",
                        "_"."t0_0" as "t0_0",
                        "_"."t1_0" as "t1_0"
                    from
                    (
                        select "_"."Freight",
                            "_"."o0",
                            "_"."t0_0",
                            "_"."t1_0"
                        from
                        (
                            select "_"."Freight" as "Freight",
                                "_"."o0" as "o0",
                                case
                                    when "_"."o0" is not null
                                    then "_"."o0"
                                    else 0
                                end as "t0_0",
                                case
                                    when "_"."o0" is null
                                    then 0
                                    else 1
                                end as "t1_0"
                            from
                            (
                                select "rows"."Freight" as "Freight",
                                    "rows"."o0" as "o0"
                                from
                                (
                                    select "_"."Freight" as "Freight",
                                        "_"."Freight" as "o0"
                                    from
                                    (
                                        select "Freight"
                                        from "public"."Orders" "$Table"
                                    ) "_"
                                    where "_"."Freight" > 50
                                ) "rows"
                                group by "Freight",
                                    "o0"
                            ) "_"
                        ) "_"
                    ) "_"
                ) "_"
                order by "_"."t0_0",
                        "_"."t1_0"
                limit 501
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);

            var rql = GetQueryString(pgQuery);
            Assert.NotNull(rql);
            // The user filter must show up in the emitted RQL. The recognizer pulls it up from the
            // intermediate wrapper into the inner query's where-clause.
            Assert.Contains("Freight", rql, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("50", rql);
            Assert.Contains(">", rql);
        }

        // A clause that merely references an aggregate-output alias (e.g. `coalesce(a0,0) > 0`, or a
        // CASE over the alias) is a real post-aggregation filter, not a redundant null-guard - so it's
        // not silently dropped. Only structural `<alias> IS NOT NULL` / `NOT (<alias> IS NULL)` guards
        // are dropped; anything else falls through to the diagnoser.

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_with_coalesce_wrapped_aggregate_alias_filter_falls_through()
        {
            // `coalesce("_"."a0", 0) > 0` is a real post-aggregation filter on the measure (a0 > 0),
            // not a null-guard. Dropping it would return unfiltered groups as if filtered, and RQL
            // can't express it post-grouping - so recognition must fall through (to the diagnoser),
            // never answer with the predicate silently dropped.
            const string sql = """
                select "_"."Employee" as "c2",
                    "_"."a0" as "a0"
                from
                (
                    select "_"."Employee",
                        "_"."a0"
                    from
                    (
                        select "rows"."Employee" as "Employee",
                            sum("rows"."Freight") as "a0"
                        from
                        (
                            from Orders
                        ) "rows"
                        group by "Employee"
                    ) "_"
                    where coalesce("_"."a0", 0) > 0
                ) "_"
                limit 1001
                """;

            Assert.False(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_with_case_wrapped_aggregate_alias_filter_falls_through()
        {
            // A CASE expression over the aggregate alias isn't a structural null-guard the recognizer
            // can prove redundant (vs. a real filter), so it isn't silently dropped: recognition falls
            // through to the diagnoser rather than risk returning data filtered differently than asked.
            const string sql = """
                select "_"."Employee" as "c2",
                    "_"."a0" as "a0"
                from
                (
                    select "_"."Employee",
                        "_"."a0"
                    from
                    (
                        select "rows"."Employee" as "Employee",
                            sum("rows"."Freight") as "a0"
                        from
                        (
                            from Orders
                        ) "rows"
                        group by "Employee"
                    ) "_"
                    where case when "_"."a0" is null then 0 else 1 end = 1
                ) "_"
                limit 1001
                """;

            Assert.False(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out _));
        }
        
        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void Two_measure_grouped_aggregate_with_or_of_null_guards_is_handled()
        {
            const string sql = """
                select "_"."Category", "_"."a0", "_"."a1"
                from
                (
                    select "rows"."Category" as "Category",
                        sum("rows"."UnitsOnOrder") as "a0",
                        sum("rows"."UnitsInStock") as "a1"
                    from "public"."Products" "rows"
                    group by "Category"
                ) "_"
                where not "_"."a0" is null or not "_"."a1" is null
                limit 1000001
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);
            Assert.Contains("group by Category", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("sum(UnitsOnOrder)", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("sum(UnitsInStock)", queryString, StringComparison.OrdinalIgnoreCase);
            // The structural null-guard must be dropped, not translated into a WHERE.
            Assert.DoesNotContain("where", queryString, StringComparison.OrdinalIgnoreCase);
        }

        // Guard against over-broadening the drop: a real measure filter (a1 > 5) OR'd with a null-guard is
        // a post-grouping HAVING that RQL can't express - it must fall through, not be silently dropped.
        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void Two_measure_grouped_aggregate_with_real_measure_filter_falls_through()
        {
            const string sql = """
                select "_"."Category", "_"."a0", "_"."a1"
                from
                (
                    select "rows"."Category" as "Category",
                        sum("rows"."UnitsOnOrder") as "a0",
                        sum("rows"."UnitsInStock") as "a1"
                    from "public"."Products" "rows"
                    group by "Category"
                ) "_"
                where not "_"."a0" is null or "_"."a1" > 5
                limit 1000001
                """;

            Assert.False(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out _));
        }

        private static string GetQueryString(PgQuery pgQuery)
        {
            return (string)typeof(PgQuery)
                .GetField("QueryString", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(pgQuery);
        }

        private static int? GetLimit(PgQuery pgQuery)
        {
            return (int?)typeof(RqlQuery)
                .GetField("_limit", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(pgQuery);
        }

        private static Dictionary<string, ReplaceColumnValue> GetReplaces(PgQuery pgQuery)
        {
            return (Dictionary<string, ReplaceColumnValue>)typeof(PowerBIRqlQuery)
                .GetField("_replaces", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(pgQuery);
        }

        private static string Normalize(string s) => s.Replace("\r\n", "\n").Trim();

        // ---- DirectQuery: CASE helper columns at outermost SELECT level ----

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_outermost_select_with_case_helper_columns_should_be_classified_as_direct_query()
        {
            // Outer SELECT contains real business columns plus CASE-based t<N>_0 helper columns.
            // The helpers must be skipped; the real columns (Employee, RequireAt) must be extracted.
            const string sql = """
                select "_"."Employee" as "c3",
                    "_"."RequireAt" as "c7",
                    case when "_"."o2" is not null then "_"."o2" else timestamp '1899-12-28 00:00:00' end as "t2_0",
                    case when "_"."o2" is null then 0 else 1 end as "t3_0"
                from
                (
                    select "rows"."Employee" as "Employee",
                        "rows"."RequireAt" as "RequireAt",
                        "rows"."RequireAt" as "o2"
                    from
                    (
                        select "Employee" as "Employee",
                            "RequireAt" as "RequireAt",
                            "RequireAt" as "o2"
                        from
                        (
                            from Orders
                            where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
                        ) "$Table"
                    ) "rows"
                    group by "Employee", "RequireAt", "o2"
                ) "_"
                order by "_"."t2_0", "_"."t3_0"
                limit 501
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);

            // Business columns must appear in the rewritten RQL; helper aliases must not.
            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);
            Assert.Contains("Employee", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("RequireAt", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("t2_0", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("t3_0", queryString, StringComparison.OrdinalIgnoreCase);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_outermost_select_with_only_helper_columns_should_NOT_classify_as_direct_query()
        {
            // Every column at every wrapper level is a helper alias (t<N>_0 or o<N>).
            // After skipping all helpers, cols.Count == 0 at each level, so TryParse must fail.
            // This validates the cols.Count > 0 guard in TryExtractOuterProjectedColumns
            // and TryExtractSimpleProjectedColumns.
            const string sql = """
                select "_"."t2_0" as "t2_0",
                    "_"."t3_0" as "t3_0"
                from
                (
                    select "_"."t2_0" as "t2_0",
                        "_"."t3_0" as "t3_0"
                    from
                    (
                        select "o2" as "o2"
                        from
                        (
                            from Orders
                            where Company = 'Companies/1-A'
                        ) "$Table"
                        group by "o2"
                    ) "_"
                ) "_"
                order by "_"."t2_0"
                limit 501
                """;

            Assert.False(PowerBIDirectQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_outer_case_expression_with_non_helper_alias_should_NOT_classify_as_direct_query()
        {
            // The outer SELECT includes a CASE expression with a non-helper alias ("delivery_status").
            // Because the alias doesn't match the t<N>_0 / o<N> helper pattern, it must NOT be skipped.
            // Classification must fail - we do not silently accept arbitrary expressions.
            //
            // Inner levels use only helper-named columns (t<N>_0, o<N>) so the
            // projection-column walker cannot find real columns at any fallback level.
            const string sql = """
                select "_"."Employee" as "c3",
                    case when "_"."RequireAt" > 5 then 'late' else 'on-time' end as "delivery_status"
                from
                (
                    select "_"."t2_0" as "t2_0",
                        "_"."o2" as "o2"
                    from
                    (
                        select "t2_0" as "t2_0",
                            "o2" as "o2"
                        from
                        (
                            from Orders
                            where Company = 'Companies/1-A'
                        ) "$Table"
                        group by "t2_0", "o2"
                    ) "_"
                ) "_"
                order by "_"."Employee"
                limit 501
                """;

            Assert.False(PowerBIDirectQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_InnerRql_4LevelWrapper_AiringNameDistinctList_Parses()
        {
            const string sql = """
                select "_"."AiringName"
                from
                (
                    select "rows"."AiringName" as "AiringName"
                    from
                    (
                        select "AiringName"
                        from
                        (
                            from "AiringSummaries"
                            select
                                AiringLegacyId,
                                AiringId,
                                ProgramId,
                                ProgramCodeId,
                                ProgramCategoryCodeId,
                                AiringName,
                                Air.AirDateTime,
                                Air.EndAirDateTime,
                                Air.Duration,
                                BroadcastAiringDetail.AirDateTime,
                                BroadcastAiringDetail.EstimatedAirDateTime,
                                BroadcastAiringDetail.ActualAirDateTime
                        ) "$Table"
                    ) "rows"
                    group by "AiringName"
                ) "_"
                order by "_"."AiringName"
                limit 501
                """;

            Assert.True(PowerBIDirectQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);

            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);
            Assert.Contains("AiringSummaries", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("AiringName", queryString, StringComparison.OrdinalIgnoreCase);
        }

        // DirectQuery: grouped count

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_grouped_count_flat_shape_inner_sql_should_parse_and_emit_count_with_no_argument()
        {
            // PowerBI "Count" visual: GROUP BY at outermost level, count() aggregate, inner SQL with SELECT *.
            // Tests: (a) flat-grouped normalizer path, (b) count() emitted without field argument,
            // (c) "as long" sort type used instead of "as double".
            const string sql = """
                select "rows"."Company" as "Company",
                    "rows"."Employee" as "Employee",
                    count("rows"."Freight") as "a0"
                from
                (
                    select *
                    from Orders
                    where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
                ) "rows"
                group by "Company",
                    "Employee"
                limit 1000001
                """;

            Assert.True(PowerBIDirectQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);

            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);
            Assert.Contains("Orders", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("count()", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Freight", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("as double", queryString, StringComparison.OrdinalIgnoreCase);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_grouped_count_flat_shape_inner_rql_should_parse_and_emit_count()
        {
            // Same flat-grouped shape, but inner is plain RQL (not SQL).
            const string sql = """
                select "rows"."Company" as "Company",
                    count("rows"."Freight") as "a0"
                from
                (
                    from Orders
                    where Company in ('Companies/1-A', 'Companies/2-A')
                ) "rows"
                group by "Company"
                limit 1000001
                """;

            Assert.True(PowerBIDirectQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);

            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);
            Assert.Contains("Orders", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("count()", queryString, StringComparison.OrdinalIgnoreCase);
        }

        // DirectQuery: grouped AVG (sum + count)

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_grouped_avg_shape_sum_plus_count_should_parse_and_emit_both_aggregates()
        {
            // PowerBI "AVG" visual: AVG is sent as SUM + COUNT so PowerBI can divide on the client.
            // Verifies: (a) TryParse succeeds, (b) classified as PowerBIDirectQuery, (c) emitted RQL
            // contains both group keys and both aggregate projections with their original aliases.
            const string sql = """
                select "rows"."Company" as "Company",
                    "rows"."Employee" as "Employee",
                    sum("rows"."Freight") as "a0",
                    count("rows"."Freight") as "a1"
                from
                (
                    select *
                from Orders
                where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
                ) "rows"
                group by "Company",
                    "Employee"
                limit 1000001
                """;

            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);

            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);
            Assert.Contains("Orders", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("group by Company, Employee", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("sum(Freight) as a0", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("count() as a1", queryString, StringComparison.OrdinalIgnoreCase);
            // Pins the known-limitation mapping: SQL count(field) -> RQL count() (row count, not non-null count).
            // Raven grouped RQL has no field-specific non-null aggregate. Matches the emitted grouped COUNT behavior.
            // See EmitGroupedAggregateRql for the full explanation; revisit if RQL gains a non-null aggregate.
            Assert.DoesNotContain("count(Freight)", queryString, StringComparison.OrdinalIgnoreCase);
        }

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_grouped_multi_aggregate_count_plus_sum_ordering_should_be_preserved()
        {
            // Swap of the AVG shape: count first, sum second. Verifies aggregate ordering follows
            // the target list, not some hard-coded sum-first convention.
            const string sql = """
                select "rows"."Company" as "Company",
                    count("rows"."Freight") as "a0",
                    sum("rows"."Freight") as "a1"
                from
                (
                    select *
                from Orders
                ) "rows"
                group by "Company"
                limit 1000001
                """;

            Assert.True(PowerBIDirectQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);

            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);
            var countIdx = queryString.IndexOf("count() as a0", StringComparison.OrdinalIgnoreCase);
            var sumIdx = queryString.IndexOf("sum(Freight) as a1", StringComparison.OrdinalIgnoreCase);
            Assert.True(countIdx > 0, "expected count() as a0 in emitted RQL: " + queryString);
            Assert.True(sumIdx > 0, "expected sum(Freight) as a1 in emitted RQL: " + queryString);
            Assert.True(countIdx < sumIdx, "aggregate order should match target-list order: " + queryString);
        }

        // DirectQuery: regression

        [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        public void DirectQuery_inner_rql_non_aggregate_wrapper_should_still_parse_correctly()
        {
            // Regression: DirectQuery with inner RQL (non-aggregate distinct-list wrapper).
            const string sql = """
                select "_"."Employee"
                from
                (
                    select "rows"."Employee" as "Employee"
                    from
                    (
                        from Orders
                        where Company in ('Companies/1-A', 'Companies/2-A')
                    ) "rows"
                    group by "Employee"
                ) "_"
                order by "_"."Employee"
                limit 501
                """;

            Assert.True(PowerBIDirectQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIDirectQuery>(pgQuery);

            var queryString = GetQueryString(pgQuery);
            Assert.NotNull(queryString);
            Assert.Contains("Orders", queryString, StringComparison.OrdinalIgnoreCase);
        }

    }
}
