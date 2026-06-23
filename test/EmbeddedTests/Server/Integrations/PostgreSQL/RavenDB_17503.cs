#if RUN_NPGSQL_TESTS
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL;

public class RavenDB_17503 : PostgreSqlIntegrationTestBase
{
    [Fact]
    public async Task DirectQuery_distinct_list_wrapper_with_inner_rql_load_two_columns_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""Name"",
        ""_"".""Manager""
from
(
    select ""rows"".""Name"" as ""Name"",
        ""rows"".""Manager"" as ""Manager""
    from
    (
        select ""Name"",
            ""Manager""
        from
        (
            from Employees as e
            where id() in ('employees/1-A')
            load e.ReportsTo as boss
            select { Name: e.FirstName, Manager: boss.FirstName }
        ) ""$Table""
    ) ""rows""
    group by ""Name"",
        ""Manager""
) ""_""
order by ""_"".""Name"",
        ""_"".""Manager""
limit 501";

            var result = await Act(store, sql);

            Assert.NotNull(result);

            Assert.True(result.Columns.Contains("Name"));
            Assert.True(result.Columns.Contains("Manager"));

            Assert.Equal(1, result.Rows.Count);

            var name = result.Rows[0]["Name"];
            var manager = result.Rows[0]["Manager"];

            Assert.NotEqual(System.DBNull.Value, name);
            Assert.NotEqual(System.DBNull.Value, manager);
        }

    }

    [Fact]
    public async Task DirectQuery_distinct_list_wrapper_single_column_employee_should_return_exactly_one_column_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""Employee""
from 
(
    select ""rows"".""Employee"" as ""Employee""
    from 
    (
        select ""Employee""
        from 
        (
            from Orders
            where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
        ) ""$Table""
    ) ""rows""
    group by ""Employee""
) ""_""
order by ""_"".""Employee""
limit 1001";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);

            Assert.True(result.Columns.Contains("Employee"));
            Assert.Equal(1, result.Columns.Count);

            foreach (DataRow row in result.Rows)
            {
                var v = row["Employee"];
                Assert.False(string.IsNullOrWhiteSpace(v.ToString()));
            }
        }
    }

    [Fact]
    public async Task DirectQuery_desktop_grouped_sum_company_orderedAt_freight_should_return_scalar_group_fields_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""Company"",
    ""_"".""OrderedAt"",
    ""_"".""a0""
from 
(
    select ""rows"".""Company"" as ""Company"",
        ""rows"".""OrderedAt"" as ""OrderedAt"",
        sum(""rows"".""Freight"") as ""a0""
    from 
    (
        from Orders
        where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
    ) ""rows""
    group by ""Company"",
        ""OrderedAt""
) ""_""
where not ""_"".""a0"" is null
limit 1000001";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);

            Assert.True(result.Columns.Contains("Company"));
            Assert.True(result.Columns.Contains("OrderedAt"));
            Assert.True(result.Columns.Contains("a0"));
            Assert.Equal(3, result.Columns.Count);

            foreach (DataRow row in result.Rows)
            {
                var company = row["Company"];
                var orderedAt = row["OrderedAt"];
                Assert.False(string.IsNullOrWhiteSpace(company.ToString()));
                Assert.False(string.IsNullOrWhiteSpace(orderedAt.ToString()));
            }
        }
    }

    [Fact]
    public async Task DirectQuery_desktop_grouped_sum_two_group_fields_with_inner_filter_on_group_field_should_work_end_to_end()
    {

        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""Employee"",
    ""_"".""RequireAt"",
    ""_"".""a0""
from
(
    select ""rows"".""Employee"" as ""Employee"",
        ""rows"".""RequireAt"" as ""RequireAt"",
        sum(""rows"".""Freight"") as ""a0""
    from
    (
        from Orders
        where Employee != null
    ) ""rows""
    group by ""Employee"",
        ""RequireAt""
) ""_""
where not ""_"".""a0"" is null
limit 1000001";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);

            Assert.Equal(3, result.Columns.Count);
            Assert.True(result.Columns.Contains("Employee"));
            Assert.True(result.Columns.Contains("RequireAt"));
            Assert.True(result.Columns.Contains("a0"));
        }
    }

    [Fact]
    public async Task DirectQuery_desktop_employee_requireAt_json_with_null_order_helper_columns_should_return_exactly_three_columns_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""Employee"" as ""c3"",
    ""_"".""RequireAt"" as ""c7"",
    ""_"".""json()"" as ""c11""
from 
(
    select ""Employee"",
        ""RequireAt"",
        ""json()"",
        ""_"".""t2_0"" as ""t2_0"",
        ""_"".""t3_0"" as ""t3_0""
    from 
    (
        select ""_"".""Employee"",
            ""_"".""RequireAt"",
            ""_"".""json()"",
            ""_"".""o2"",
            ""_"".""t2_0"",
            ""_"".""t3_0""
        from 
        (
            select ""_"".""Employee"" as ""Employee"",
                ""_"".""RequireAt"" as ""RequireAt"",
                ""_"".""json()"" as ""json()"",
                ""_"".""o2"" as ""o2"",
                case
                    when ""_"".""o2"" is not null
                    then ""_"".""o2""
                    else timestamp '1899-12-28 00:00:00'
                end as ""t2_0"",
                case
                    when ""_"".""o2"" is null
                    then 0
                    else 1
                end as ""t3_0""
            from 
            (
                select ""rows"".""Employee"" as ""Employee"",
                    ""rows"".""RequireAt"" as ""RequireAt"",
                    ""rows"".""json()"" as ""json()"",
                    ""rows"".""o2"" as ""o2""
                from 
                (
                    select ""Employee"" as ""Employee"",
                        ""RequireAt"" as ""RequireAt"",
                        ""json()"" as ""json()"",
                        ""RequireAt"" as ""o2""
                    from 
                    (
                        from Orders
                        where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
                    ) ""$Table""
                ) ""rows""
                group by ""Employee"",
                    ""RequireAt"",
                    ""json()"",
                    ""o2""
            ) ""_""
        ) ""_""
    ) ""_""
) ""_""
order by ""_"".""Employee"",
        ""_"".""json()"",
        ""_"".""t2_0"",
        ""_"".""t3_0""
limit 501";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);

            Assert.True(result.Columns.Contains("Employee"));
            Assert.True(result.Columns.Contains("RequireAt"));
            Assert.True(result.Columns.Contains("json()"));
            Assert.Equal(3, result.Columns.Count);

            foreach (DataRow row in result.Rows)
            {
                Assert.NotNull(row["Employee"]);
                Assert.NotNull(row["RequireAt"]);
                Assert.NotNull(row["json()"]);
            }
        }
    }

    [Fact]
    public async Task DirectQuery_distinct_list_wrapper_single_column_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""Name""
from 
(
    select ""rows"".""Name"" as ""Name""
    from 
    (
        select ""Name""
        from 
        (
            declare function name(e) {
    return e.FirstName + "" "" + e.LastName
}
from Employees as e
where id() in ('employees/1-A'  )
select { Name: name(e)}
        ) ""$Table""
    ) ""rows""
    group by ""Name""
) ""_""
order by ""_"".""Name""
limit 501";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.Equal(1, result.Rows.Count);
            Assert.Equal("Nancy Davolio", result.Rows[0]["Name"]);
        }
    }

    [Fact]
    public async Task DirectQuery_distinct_list_wrapper_two_columns_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""Name"",
        ""_"".""Title""
    from
    (
        select ""rows"".""Name"" as ""Name"",
            ""rows"".""Title"" as ""Title""
        from
        (
            select ""Name"",
                ""Title""
            from
            (
                declare function name(e) {
        return e.FirstName + "" "" + e.LastName
    }
    from Employees as e
    where id() in ('employees/1-A','employees/2-A','employees/3-A')
    select { Name: name(e), Title: e.Title}
            ) ""$Table""
        ) ""rows""
        group by ""Name"",
            ""Title""
    ) ""_""
    order by ""_"".""Name"",
            ""_"".""Title""
    limit 501";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.Equal(3, result.Rows.Count);

            Assert.True(result.Columns.Contains("Name"));
            Assert.True(result.Columns.Contains("Title"));

            var actual = new HashSet<(string Name, string Title)>();
            foreach (DataRow row in result.Rows)
            {
                Assert.True(row.Table.Columns.Contains("Name"));
                Assert.True(row.Table.Columns.Contains("Title"));

                Assert.NotNull(row["Name"]);
                Assert.NotNull(row["Title"]);

                Assert.NotEqual(System.DBNull.Value, row["Name"]);
                Assert.NotEqual(System.DBNull.Value, row["Title"]);

                actual.Add(((string)row["Name"], (string)row["Title"]));
            }

            Assert.Contains(("Nancy Davolio", "Sales Representative"), actual);
            Assert.Contains(("Andrew Fuller", "Vice President, Sales"), actual);
            Assert.Contains(("Janet Leverling", "Sales Representative"), actual);
        }
    }

    [Fact]
    public async Task DirectQuery_distinct_list_wrapper_order_by_null_helper_columns_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""OrderedAt"" as ""c6"",
    ""_"".""RequireAt"" as ""c7""
from
(
    select ""OrderedAt"",
        ""RequireAt"",
        ""_"".""t0_0"" as ""t0_0"",
        ""_"".""t1_0"" as ""t1_0"",
        ""_"".""t2_0"" as ""t2_0"",
        ""_"".""t3_0"" as ""t3_0""
    from
    (
        select ""_"".""OrderedAt"",
            ""_"".""RequireAt"",
            ""_"".""o0"",
            ""_"".""o1"",
            ""_"".""t0_0"",
            ""_"".""t1_0"",
            ""_"".""t2_0"",
            ""_"".""t3_0""
        from
        (
            select ""_"".""OrderedAt"" as ""OrderedAt"",
                ""_"".""RequireAt"" as ""RequireAt"",
                ""_"".""o0"" as ""o0"",
                ""_"".""o1"" as ""o1"",
                case
                    when ""_"".""o0"" is not null
                    then ""_"".""o0""
                    else timestamp '1899-12-28 00:00:00'
                end as ""t0_0"",
                case
                    when ""_"".""o0"" is null
                    then 0
                    else 1
                end as ""t1_0"",
                case
                    when ""_"".""o1"" is not null
                    then ""_"".""o1""
                    else timestamp '1899-12-28 00:00:00'
                end as ""t2_0"",
                case
                    when ""_"".""o1"" is null
                    then 0
                    else 1
                end as ""t3_0""
            from
            (
                select ""rows"".""OrderedAt"" as ""OrderedAt"",
                    ""rows"".""RequireAt"" as ""RequireAt"",
                    ""rows"".""o0"" as ""o0"",
                    ""rows"".""o1"" as ""o1""
                from
                (
                    select ""OrderedAt"" as ""OrderedAt"",
                        ""RequireAt"" as ""RequireAt"",
                        ""RequireAt"" as ""o0"",
                        ""OrderedAt"" as ""o1""
                    from
                    (
                        from Orders as o
                        where o.Company = ""Companies/1-A"" OR o.Company = ""Companies/2-A""
                    ) ""$Table""
                ) ""rows""
                group by ""OrderedAt"",
                    ""RequireAt"",
                    ""o0"",
                    ""o1""
            ) ""_""
        ) ""_""
    ) ""_""
) ""_""
order by ""_"".""t0_0"",
        ""_"".""t1_0"",
        ""_"".""t2_0"",
        ""_"".""t3_0""
limit 501";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.True(result.Rows.Count > 0);
            Assert.True(result.Columns.Contains("OrderedAt"));
            Assert.True(result.Columns.Contains("RequireAt"));

            var orderedAt = result.Rows[0]["OrderedAt"];
            var requireAt = result.Rows[0]["RequireAt"];
            Assert.True(orderedAt != System.DBNull.Value || requireAt != System.DBNull.Value);
        }
    }

    [Fact]
    public async Task DirectQuery_distinct_list_wrapper_order_by_null_helper_columns_desc_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""OrderedAt"" as ""c6"",
    ""_"".""RequireAt"" as ""c7""
from
(
    select ""OrderedAt"",
        ""RequireAt"",
        ""_"".""t0_0"" as ""t0_0"",
        ""_"".""t1_0"" as ""t1_0"",
        ""_"".""t2_0"" as ""t2_0"",
        ""_"".""t3_0"" as ""t3_0""
    from
    (
        select ""_"".""OrderedAt"",
            ""_"".""RequireAt"",
            ""_"".""o0"",
            ""_"".""o1"",
            ""_"".""t0_0"",
            ""_"".""t1_0"",
            ""_"".""t2_0"",
            ""_"".""t3_0""
        from
        (
            select ""_"".""OrderedAt"" as ""OrderedAt"",
                ""_"".""RequireAt"" as ""RequireAt"",
                ""_"".""o0"" as ""o0"",
                ""_"".""o1"" as ""o1"",
                case
                    when ""_"".""o0"" is not null
                    then ""_"".""o0""
                    else timestamp '1899-12-28 00:00:00'
                end as ""t0_0"",
                case
                    when ""_"".""o0"" is null
                    then 0
                    else 1
                end as ""t1_0"",
                case
                    when ""_"".""o1"" is not null
                    then ""_"".""o1""
                    else timestamp '1899-12-28 00:00:00'
                end as ""t2_0"",
                case
                    when ""_"".""o1"" is null
                    then 0
                    else 1
                end as ""t3_0""
            from
            (
                select ""rows"".""OrderedAt"" as ""OrderedAt"",
                    ""rows"".""RequireAt"" as ""RequireAt"",
                    ""rows"".""o0"" as ""o0"",
                    ""rows"".""o1"" as ""o1""
                from
                (
                    select ""OrderedAt"" as ""OrderedAt"",
                        ""RequireAt"" as ""RequireAt"",
                        ""RequireAt"" as ""o0"",
                        ""OrderedAt"" as ""o1""
                    from
                    (
                        from Orders as o
                        where o.Company = ""Companies/1-A"" OR o.Company = ""Companies/2-A""
                    ) ""$Table""
                ) ""rows""
                group by ""OrderedAt"",
                    ""RequireAt"",
                    ""o0"",
                    ""o1""
            ) ""_""
        ) ""_""
    ) ""_""
) ""_""
order by ""_"".""t0_0"" desc,
        ""_"".""t1_0"" desc,
        ""_"".""t2_0"" desc,
        ""_"".""t3_0"" desc
limit 501";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.True(result.Rows.Count > 0);
            Assert.True(result.Columns.Contains("OrderedAt"));
            Assert.True(result.Columns.Contains("RequireAt"));
        }
    }

    [Fact]
    public async Task DirectQuery_distinct_list_wrapper_order_by_null_helper_columns_from_projected_inner_rql_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""OrderedAt"" as ""c2"",
    ""_"".""RequireAt"" as ""c3""
from
(
    select ""OrderedAt"",
        ""RequireAt"",
        ""_"".""t0_0"" as ""t0_0"",
        ""_"".""t1_0"" as ""t1_0"",
        ""_"".""t2_0"" as ""t2_0"",
        ""_"".""t3_0"" as ""t3_0""
    from
    (
        select ""_"".""OrderedAt"",
            ""_"".""RequireAt"",
            ""_"".""o0"",
            ""_"".""o1"",
            ""_"".""t0_0"",
            ""_"".""t1_0"",
            ""_"".""t2_0"",
            ""_"".""t3_0""
        from
        (
            select ""_"".""OrderedAt"" as ""OrderedAt"",
                ""_"".""RequireAt"" as ""RequireAt"",
                ""_"".""o0"" as ""o0"",
                ""_"".""o1"" as ""o1"",
                case
                    when ""_"".""o0"" is not null
                    then ""_"".""o0""
                    else timestamp '1899-12-28 00:00:00'
                end as ""t0_0"",
                case
                    when ""_"".""o0"" is null
                    then 0
                    else 1
                end as ""t1_0"",
                case
                    when ""_"".""o1"" is not null
                    then ""_"".""o1""
                    else timestamp '1899-12-28 00:00:00'
                end as ""t2_0"",
                case
                    when ""_"".""o1"" is null
                    then 0
                    else 1
                end as ""t3_0""
            from
            (
                select ""rows"".""OrderedAt"" as ""OrderedAt"",
                    ""rows"".""RequireAt"" as ""RequireAt"",
                    ""rows"".""o0"" as ""o0"",
                    ""rows"".""o1"" as ""o1""
                from
                (
                    select ""OrderedAt"" as ""OrderedAt"",
                        ""RequireAt"" as ""RequireAt"",
                        ""OrderedAt"" as ""o0"",
                        ""RequireAt"" as ""o1""
                    from
                    (
                        from Orders as o
                        where o.Company = ""Companies/1-A"" OR o.Company = ""Companies/2-A""
                        select { OrderedAt: o.OrderedAt, RequireAt: o.RequireAt}
                    ) ""$Table""
                ) ""rows""
                group by ""OrderedAt"",
                    ""RequireAt"",
                    ""o0"",
                    ""o1""
            ) ""_""
        ) ""_""
    ) ""_""
) ""_""
order by ""_"".""t0_0"",
        ""_"".""t1_0"",
        ""_"".""t2_0"",
        ""_"".""t3_0""
limit 501";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.True(result.Rows.Count > 0);
            Assert.True(result.Columns.Contains("OrderedAt"));
            Assert.True(result.Columns.Contains("RequireAt"));
        }
    }

    [Fact]
    public async Task DirectQuery_select_id_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""id()""
from
(
    select ""rows"".""id()"" as ""id()""
    from
    (
        select ""id()""
        from
        (
            from Employees as e
            where id() in ('employees/1-A')
            select { id(): id(e) }
        ) ""$Table""
    ) ""rows""
    group by ""id()""
) ""_""
order by ""_"".""id()""
limit 501";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.Equal(1, result.Rows.Count);
            Assert.True(result.Columns.Contains("id()"));

            var id = result.Rows[0]["id()"]?.ToString();
            Assert.Equal("employees/1-A", id);
        }
    }

    [Fact]
    public async Task DirectQuery_select_json_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""json()""
from
(
    select ""rows"".""json()"" as ""json()""
    from
    (
        select ""json()""
        from
        (
            from Employees as e
            where id() in ('employees/1-A')
            select { ""json()"": e }
        ) ""$Table""
    ) ""rows""
    group by ""json()""
) ""_""
order by ""_"".""json()""
limit 501";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.Equal(1, result.Rows.Count);
            Assert.True(result.Columns.Contains("json()"));

            var json = result.Rows[0]["json()"]?.ToString();
            Assert.NotNull(json);
        }
    }

    [Fact]
    public async Task DirectQuery_grouped_sum_wrapper_from_desktop_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""Company"" as ""c2"",
    ""_"".""a0"" as ""a0""
from
(
    select ""_"".""Company"",
        ""_"".""a0""
    from
    (
        select ""_"".""Company"",
            ""_"".""a0""
        from
        (
            select ""rows"".""Company"" as ""Company"",
                sum(""rows"".""Freight"") as ""a0""
            from
            (
                select ""Company"",
                    ""Freight""
                from
                (
                    from Orders as o
                    where o.Company = ""Companies/1-A"" OR o.Company = ""Companies/2-A""
                    select { Company: o.Company, Freight: o.Freight}
                ) ""$Table""
            ) ""rows""
            group by ""Company""
        ) ""_""
        where not ""_"".""a0"" is null
    ) ""_""
) ""_""
order by ""_"".""a0"" desc,
        ""_"".""Company""
limit 1001";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.True(result.Rows.Count > 0);
            Assert.True(result.Columns.Contains("Company"));
            Assert.True(result.Columns.Contains("a0"));
        }
    }

    [Fact]
    public async Task DirectQuery_desktop_grouped_sum_company_orderedAt_freight_where_company_is_group_key_should_return_exact_values_end_to_end()
    {
        // Two-query approach: first fetch raw rows and compute the ground-truth sums in C#,
        // then run the grouped-sum DirectQuery and verify each row matches exactly.
        // This avoids hard-coding expected values that depend on the sample dataset.
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            // Step 1: fetch the raw rows that will be grouped.
            // Uses the non-aggregate simple-fetch path so we get the real document values.
            const string rawSql = @"select ""_"".""Company"",
    ""_"".""OrderedAt"",
    ""_"".""Freight""
from
(
    from Orders
    where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
) ""_""
limit 1000001";

            var rawResult = await Act(store, rawSql);
            Assert.NotNull(rawResult);
            Assert.NotEmpty(rawResult.Rows);

            // Compute ground-truth sums grouped by (Company, OrderedAt).
            var expected = new Dictionary<(string Company, string OrderedAt), decimal>();
            foreach (DataRow row in rawResult.Rows)
            {
                var company = row["Company"].ToString();
                var orderedAt = row["OrderedAt"].ToString();
                var freight = Convert.ToDecimal(row["Freight"]);
                var key = (company, orderedAt);
                expected.TryGetValue(key, out var existing);
                expected[key] = existing + freight;
            }

            Assert.NotEmpty(expected);

            // Step 2: run the grouped-sum DirectQuery and verify against ground truth.
            const string sql = @"select ""_"".""Company"",
    ""_"".""OrderedAt"",
    ""_"".""a0""
from
(
    select ""rows"".""Company"" as ""Company"",
        ""rows"".""OrderedAt"" as ""OrderedAt"",
        sum(""rows"".""Freight"") as ""a0""
    from
    (
        from Orders
        where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
    ) ""rows""
    group by ""Company"",
        ""OrderedAt""
) ""_""
where not ""_"".""a0"" is null
limit 1000001";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.Equal(3, result.Columns.Count);
            Assert.True(result.Columns.Contains("Company"));
            Assert.True(result.Columns.Contains("OrderedAt"));
            Assert.True(result.Columns.Contains("a0"));
            Assert.Equal(expected.Count, result.Rows.Count);

            foreach (DataRow row in result.Rows)
            {
                var company = row["Company"].ToString();
                var orderedAt = row["OrderedAt"].ToString();
                var freight = Convert.ToDecimal(row["a0"]);
                var key = (company, orderedAt);
                Assert.True(expected.ContainsKey(key),
                    $"Unexpected group (Company={company}, OrderedAt={orderedAt}) in result");
                var expectedFreight = expected[key];
                Assert.True(Math.Abs(freight - expectedFreight) < 0.01m,
                    $"Freight mismatch for (Company={company}, OrderedAt={orderedAt}): expected {expectedFreight}, got {freight}");
            }
        }
    }

    [Fact(Skip = "Unsupported: Raven grouped-RQL WHERE is HAVING (group-keys/aggregates only). Pre-group filter on non-group-key field (Company) cannot be expressed.")]
    public async Task DirectQuery_desktop_grouped_sum_two_group_fields_with_outer_where_not_null_should_work_end_to_end()
    {
        // The inner RQL filters on Company (not a group key) before grouping by Employee + RequireAt.
        // Raven grouped-RQL WHERE is a HAVING clause; it only allows group-key and aggregate expressions.
        // There is no pre-aggregate filter syntax in Raven RQL for grouped queries.

        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select ""_"".""Employee"",
    ""_"".""RequireAt"",
    ""_"".""a0""
from 
(
    select ""rows"".""Employee"" as ""Employee"",
        ""rows"".""RequireAt"" as ""RequireAt"",
        sum(""rows"".""Freight"") as ""a0""
    from 
    (
        from Orders
        where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
    ) ""rows""
    group by ""Employee"",
        ""RequireAt""
) ""_""
limit 1000001";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);

            Assert.Equal(3, result.Columns.Count);
            Assert.True(result.Columns.Contains("Employee"));
            Assert.True(result.Columns.Contains("RequireAt"));
            Assert.True(result.Columns.Contains("a0"));
        }
    }

    [Fact]
    public async Task DirectQuery_grouped_count_flat_shape_inner_sql_should_work_end_to_end()
    {
        // PowerBI "Count" visual: flat grouped shape - GROUP BY at outermost select level,
        // count() aggregate, inner SQL with SELECT *.
        // Verifies: (a) TryParse succeeds, (b) Raven executes and returns rows,
        // (c) the count column values equal the number of matching documents per group.
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            // Step 1: fetch raw rows to compute expected counts client-side.
            const string rawSql = @"select ""_"".""Company"",
    ""_"".""Employee""
from
(
    from Orders
    where ""Company"" in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
) ""_""
limit 1000001";

            var rawResult = await Act(store, rawSql);
            Assert.NotNull(rawResult);
            Assert.NotEmpty(rawResult.Rows);

            var expected = new Dictionary<(string Company, string Employee), int>();
            foreach (DataRow row in rawResult.Rows)
            {
                var company = row["Company"].ToString();
                var employee = row["Employee"].ToString();
                var key = (company, employee);
                expected.TryGetValue(key, out var existing);
                expected[key] = existing + 1;
            }

            Assert.NotEmpty(expected);

            // Step 2: run the flat grouped count query.
            const string sql =
                @"select ""rows"".""Company"" as ""Company"",
    ""rows"".""Employee"" as ""Employee"",
    count(""rows"".""Freight"") as ""a0""
from
(
    select *
    from Orders
    where ""Company"" in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
) ""rows""
group by ""Company"",
    ""Employee""
limit 1000001";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.True(result.Columns.Contains("Company"));
            Assert.True(result.Columns.Contains("Employee"));
            Assert.True(result.Columns.Contains("a0"));

            foreach (DataRow row in result.Rows)
            {
                var company = row["Company"].ToString();
                var employee = row["Employee"].ToString();
                var countVal = Convert.ToInt64(row["a0"]);

                Assert.True(expected.TryGetValue((company, employee), out var expectedCount),
                    $"Unexpected group ({company}, {employee}) in result");
                Assert.Equal(expectedCount, (int)countVal);
            }

            Assert.Equal(expected.Count, result.Rows.Count);
        }
    }

    [Fact]
    public async Task DirectQuery_grouped_avg_flat_shape_sum_plus_count_should_work_end_to_end()
    {
        // PowerBI "AVG" visual is sent as SUM + COUNT so that PowerBI can compute AVG client-side.
        // Verifies: (a) TryParse succeeds via the multi-aggregate grouped path,
        // (b) Raven executes and returns rows,
        // (c) each row's sum/count matches the ground truth computed from raw rows,
        // (d) the client-side AVG (a0 / a1) equals the ground-truth average per group.
        //
        // KNOWN LIMITATION - the "count" column here is the group's ROW count (Raven count()),
        // not SQL's non-null count. Because the sample dataset's Freight is non-null on every
        // order, the two values coincide and AVG = SUM/COUNT is correct here. If Freight ever
        // contained nulls, the COUNT value would over-count vs. SQL semantics and AVG would be
        // off. See EmitGroupedAggregateRql for the full note on this mapping.
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            // Step 1: fetch raw rows to compute expected sums and counts client-side.
            const string rawSql = @"select ""_"".""Company"",
    ""_"".""Employee"",
    ""_"".""Freight""
from
(
    from Orders
    where ""Company"" in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
) ""_""
limit 1000001";

            var rawResult = await Act(store, rawSql);
            Assert.NotNull(rawResult);
            Assert.NotEmpty(rawResult.Rows);

            var expectedSum = new Dictionary<(string Company, string Employee), decimal>();
            var expectedCount = new Dictionary<(string Company, string Employee), int>();
            foreach (DataRow row in rawResult.Rows)
            {
                var company = row["Company"].ToString();
                var employee = row["Employee"].ToString();
                var freight = Convert.ToDecimal(row["Freight"]);
                var key = (company, employee);
                expectedSum.TryGetValue(key, out var existingSum);
                expectedSum[key] = existingSum + freight;
                expectedCount.TryGetValue(key, out var existingCount);
                expectedCount[key] = existingCount + 1;
            }

            Assert.NotEmpty(expectedSum);

            // Step 2: run the AVG-shape (sum + count) query.
            const string sql =
                @"select ""rows"".""Company"" as ""Company"",
    ""rows"".""Employee"" as ""Employee"",
    sum(""rows"".""Freight"") as ""a0"",
    count(""rows"".""Freight"") as ""a1""
from
(
    select *
    from Orders
    where ""Company"" in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
) ""rows""
group by ""Company"",
    ""Employee""
limit 1000001";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.True(result.Columns.Contains("Company"));
            Assert.True(result.Columns.Contains("Employee"));
            Assert.True(result.Columns.Contains("a0"));
            Assert.True(result.Columns.Contains("a1"));

            Assert.Equal(expectedSum.Count, result.Rows.Count);

            foreach (DataRow row in result.Rows)
            {
                var company = row["Company"].ToString();
                var employee = row["Employee"].ToString();
                var sumVal = Convert.ToDecimal(row["a0"]);
                var countVal = Convert.ToInt64(row["a1"]);
                var key = (company, employee);

                Assert.True(expectedSum.TryGetValue(key, out var expSum),
                    $"Unexpected group ({company}, {employee}) in result");
                Assert.True(Math.Abs(sumVal - expSum) < 0.01m,
                    $"SUM mismatch for ({company}, {employee}): expected {expSum}, got {sumVal}");

                Assert.True(expectedCount.TryGetValue(key, out var expCount));
                Assert.Equal(expCount, (int)countVal);
            }
        }
    }

    [Fact(Skip = "Unsupported: scalar sum() without group by is rejected by Raven RQL.")]
    public async Task DirectQuery_aggregate_only_sum_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select sum(""rows"".""Freight"") as ""a0""
from
(
    select ""Freight""
    from
    (
        from Orders
    ) ""$Table""
) ""rows""";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.Equal(1, result.Rows.Count);
            Assert.True(result.Columns.Contains("a0"));

            var raw = result.Rows[0]["a0"];
            Assert.NotNull(raw);
            Assert.NotEqual(System.DBNull.Value, raw);
            Assert.True(decimal.TryParse(raw.ToString(), out var sum));
            Assert.True(sum > 0);
        }
    }

    [Fact(Skip = "Unsupported DirectQuery wrapper family: count(distinct(...)) + max(...) summary-card shape.")]
    public async Task DirectQuery_summary_card_count_distinct_with_max_helpers_is_not_supported_yet()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql = @"select count(distinct(""rows"".""OrderedAt"")) + max(""rows"".""08aba381d6ce4b7ca6b81ef186373995..1"") as ""a0"",
    count(distinct(""rows"".""Company"")) + max(""rows"".""08aba381d6ce4b7ca6b81ef186373995..2"") as ""a1"",
    count(distinct(""rows"".""RequireAt"")) + max(""rows"".""08aba381d6ce4b7ca6b81ef186373995..3"") as ""a2""
from 
(
    select ""_"".""Company"" as ""Company"",
        ""_"".""OrderedAt"" as ""OrderedAt"",
        ""_"".""RequireAt"" as ""RequireAt"",
        case
            when ""_"".""OrderedAt"" is null
            then 1
            else 0
        end as ""08aba381d6ce4b7ca6b81ef186373995..1"",
        case
            when ""_"".""Company"" is null
            then 1
            else 0
        end as ""08aba381d6ce4b7ca6b81ef186373995..2"",
        case
            when ""_"".""RequireAt"" is null
            then 1
            else 0
        end as ""08aba381d6ce4b7ca6b81ef186373995..3""
    from 
    (
        select ""Company"",
            ""OrderedAt"",
            ""RequireAt""
        from 
        (
            from Orders
            where Company in ('Companies/1-A', 'Companies/2-A', 'Companies/3-A')
        ) ""$Table""
    ) ""_""
) ""rows""";

            var result = await Act(store, sql);

            Assert.NotNull(result);
        }
    }

}

#endif
