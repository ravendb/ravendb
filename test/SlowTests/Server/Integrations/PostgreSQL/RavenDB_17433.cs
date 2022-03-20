using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Integrations.PostgreSQL
{
    public class RavenDB_17433 : PostgreSqlIntegrationTestBase
    {
        public RavenDB_17433(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task QueryWithSingleReplaceShouldWork()
        {
            const string queryWithSingleReplace = @"select ""_"".""id()"" as ""id()"",
    ""_"".""LastName"" as ""LastName"",
    ""_"".""FirstName"" as ""FirstName"",
    ""_"".""Title"" as ""Title"",
    ""_"".""Address"" as ""Address"",
    ""_"".""HiredAt"" as ""HiredAt"",
    ""_"".""Birthday"" as ""Birthday"",
    ""_"".""HomePhone"" as ""HomePhone"",
    ""_"".""Extension"" as ""Extension"",
    ""_"".""ReportsTo"" as ""ReportsTo"",
    ""_"".""Notes"" as ""Notes"",
    ""_"".""Territories"" as ""Territories"",
    ""_"".""json()"" as ""json()"",
    replace(""_"".""Title"", 'Sales', 'Marketing') as ""t0_0""
from
(
    from Employees where startsWith(LastName, 'D') select Title
) ""_""
limit 1000";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                var result = await Act(store, queryWithSingleReplace, Server);

                DataRowCollection rows = result.Rows;

                Assert.Equal(2, rows.Count);

                var updatedTitles = result
                    .AsEnumerable()
                    .Select(x => x.Field<string>("t0_0"))
                    .ToList();

                Assert.Equal("Marketing Representative", updatedTitles[0]);
                Assert.Equal("Marketing Representative", updatedTitles[1]);
            }
        }

        [Fact]
        public async Task QueryWithMultipleNestedReplacesShouldWork()
        {
            const string queryWithMultipleNestedReplaces = @"select ""_"".""id()"" as ""id()"",
    ""_"".""LastName"" as ""LastName"",
    ""_"".""FirstName"" as ""FirstName"",
    ""_"".""Title"" as ""Title"",
    ""_"".""Address"" as ""Address"",
    ""_"".""HiredAt"" as ""HiredAt"",
    ""_"".""Birthday"" as ""Birthday"",
    ""_"".""HomePhone"" as ""HomePhone"",
    ""_"".""Extension"" as ""Extension"",
    ""_"".""ReportsTo"" as ""ReportsTo"",
    ""_"".""Notes"" as ""Notes"",
    ""_"".""Territories"" as ""Territories"",
    ""_"".""json()"" as ""json()"",
    ""_"".""t0_0"" as ""t0_0"",
    ""_"".""t0_03"" as ""t0_03"",
    replace(""_"".""t0_0"", 'aaa', 'bbb') as ""t0_02"",
    replace(""_"".""t0_03"", 'Steven', 'ddd') as ""t0_04""
from
(
    select ""_"".""id()"" as ""id()"",
        ""_"".""LastName"" as ""LastName"",
        ""_"".""FirstName"" as ""FirstName"",
        ""_"".""Title"" as ""Title"",
        ""_"".""Address"" as ""Address"",
        ""_"".""HiredAt"" as ""HiredAt"",
        ""_"".""Birthday"" as ""Birthday"",
        ""_"".""HomePhone"" as ""HomePhone"",
        ""_"".""Extension"" as ""Extension"",
        ""_"".""ReportsTo"" as ""ReportsTo"",
        ""_"".""Notes"" as ""Notes"",
        ""_"".""Territories"" as ""Territories"",
        ""_"".""json()"" as ""json()"",
        replace(""_"".""LastName"", 'Dodsworth', 'aaa') as ""t0_0"",
        replace(""_"".""FirstName"", 'Janet', 'ccc') as ""t0_03""
    from
    (
        from Employees
    ) ""_""
) ""_""
limit 1000";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                var result2 = await Act(store, queryWithMultipleNestedReplaces, Server);

                DataRowCollection rows2 = result2.Rows;

                Assert.Equal(9, rows2.Count);

                var updatedLastNames = result2
                    .AsEnumerable()
                    .Select(x => x.Field<string>("t0_02"))
                    .ToList();

                Assert.Contains("bbb", updatedLastNames);
                Assert.DoesNotContain("aaa", updatedLastNames);


                var updatedFirstNames = result2
                    .AsEnumerable()
                    .Select(x => x.Field<string>("t0_04"))
                    .ToList();

                Assert.Contains("ccc", updatedFirstNames);
                Assert.Contains("ddd", updatedFirstNames);
            }
        }

        [Fact]
        public async Task QueryWithSingleWhereFilteringShouldWork()
        {
            const string queryWithSingleWhereCondition = @"select ""_"".""id()"",
    ""_"".""LastName"",
    ""_"".""FirstName"",
    ""_"".""Title"",
    ""_"".""Address"",
    ""_"".""HiredAt"",
    ""_"".""Birthday"",
    ""_"".""HomePhone"",
    ""_"".""Extension"",
    ""_"".""ReportsTo"",
    ""_"".""Notes"",
    ""_"".""Territories"",
    ""_"".""json()""
from
(
    from Employees where startsWith(LastName, 'D')
) ""_""
where ""_"".""FirstName"" = 'Anne' and ""_"".""FirstName"" is not null
limit 1000";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                // queryWithSingleReplace

                var result = await Act(store, queryWithSingleWhereCondition, Server);

                DataRowCollection rows = result.Rows;

                Assert.Equal(1, rows.Count);
            }
        }

        [Fact]
        public async Task QueryWithSingleWhereFilteringAndSelectShouldWork()
        {
            const string queryWithSingleWhereCondition = @"select ""_"".""id()"",
    ""_"".""LastName"",
    ""_"".""FirstName"",
    ""_"".""Title"",
    ""_"".""Address"",
    ""_"".""HiredAt"",
    ""_"".""Birthday"",
    ""_"".""HomePhone"",
    ""_"".""Extension"",
    ""_"".""ReportsTo"",
    ""_"".""Notes"",
    ""_"".""Territories"",
    ""_"".""json()""
from
(
    from Employees as o select { 
    LastModified: o[""@metadata""][""@last-modified""], 
    Name: o.FirstName + "" "" + o.LastName
}
) ""_""
where ""_"".""FirstName"" = 'Anne' and ""_"".""FirstName"" is not null
limit 1000";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                // queryWithSingleReplace

                var result = await Act(store, queryWithSingleWhereCondition, Server);

                DataRowCollection rows = result.Rows;

                Assert.Equal(1, rows.Count);
            }
        }

        [Fact]
        public async Task QueryWithSingleWhereFilteringAndAliasShouldWork()
        {
            const string queryWithSingleWhereCondition = @"select ""_"".""id()"",
    ""_"".""FirstName"",
    ""_"".""LastName"",
    ""_"".""json()""
from
(
    FROM Employees as e WHERE startsWith(e.LastName, 'D') select e.FirstName, e.LastName
) ""_""
where ""_"".""LastName"" = 'Dodsworth' and ""_"".""LastName"" is not null
limit 1000";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                // queryWithSingleReplace

                var result = await Act(store, queryWithSingleWhereCondition, Server);

                DataRowCollection rows = result.Rows;

                Assert.Equal(1, rows.Count);
            }
        }

        [Fact]
        public async Task QueryWithMultipleWhereFilteringShouldWork()
        {
            const string query = @"select ""_"".""id()"",
    ""_"".""LastName"",
    ""_"".""FirstName"",
    ""_"".""Title"",
    ""_"".""Address"",
    ""_"".""HiredAt"",
    ""_"".""Birthday"",
    ""_"".""HomePhone"",
    ""_"".""Extension"",
    ""_"".""ReportsTo"",
    ""_"".""Notes"",
    ""_"".""Territories"",
    ""_"".""json()""
from
(
    from Employees
) ""_""
where ((""_"".""FirstName"" <> 'Anne' or ""_"".""FirstName"" is null) and (""_"".""FirstName"" <> 'Janet' or ""_"".""FirstName"" is null)) and (""_"".""Title"" = 'Inside Sales Coordinator' and ""_"".""Title"" is not null or ""_"".""Title"" = 'Vice President, Sales' and ""_"".""Title"" is not null)
limit 1000";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                // queryWithSingleReplace

                var result = await Act(store, query, Server);

                DataRowCollection rows = result.Rows;

                Assert.Equal(2, rows.Count);
            }
        }

        [Fact]
        public async Task QueryWithDateTimeConditionFilteringShouldWork()
        {
            const string query = @"select ""_"".""id()"",
    ""_"".""LastName"",
    ""_"".""FirstName"",
    ""_"".""Title"",
    ""_"".""Address"",
    ""_"".""HiredAt"",
    ""_"".""Birthday"",
    ""_"".""HomePhone"",
    ""_"".""Extension"",
    ""_"".""ReportsTo"",
    ""_"".""Notes"",
    ""_"".""Territories"",
    ""_"".""json()""
from
(
    from Employees where startsWith(LastName, 'D')
) ""_""
where ""_"".""HiredAt"" = timestamp '1994-11-15 00:00:00' and ""_"".""HiredAt"" is not null
limit 1000";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                // queryWithSingleReplace

                var result = await Act(store, query, Server);

                DataRowCollection rows = result.Rows;

                Assert.Equal(1, rows.Count);
            }
        }

        [Fact]
        public async Task QueryWithTwoDateTimeConditionsFilteringShouldWork()
        {
            const string query = @"select ""_"".""id()"",
    ""_"".""Company"",
    ""_"".""Employee"",
    ""_"".""Freight"",
    ""_"".""Lines"",
    ""_"".""OrderedAt"",
    ""_"".""RequireAt"",
    ""_"".""ShipTo"",
    ""_"".""ShipVia"",
    ""_"".""ShippedAt"",
    ""_"".""json()""
from
(
    from Orders
) ""_""
where ""_"".""OrderedAt"" >= timestamp '1995-10-10 00:00:00' and ""_"".""OrderedAt"" < timestamp '1997-10-10 00:00:00'
limit 1000";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                using (var session = store.OpenAsyncSession())
                {
                    var orders = await session
                        .Query<Order>().Where(x => x.OrderedAt > new DateTime(1995, 10, 10) && x.OrderedAt < new DateTime(1997, 10, 10))
                        .ToListAsync();

                    var result = await Act(store, query, Server);

                    DataRowCollection rows = result.Rows;

                    Assert.Equal(orders.Count, rows.Count);
                }
            }
        }

        [Fact]
        public async Task ComplexQueryWithReplaceAndMultipleNestedFilteringShouldWork()
        {
            const string query = @"select ""_"".""id()"" as ""id()"",
    ""_"".""LastName"" as ""LastName"",
    ""_"".""FirstName"" as ""FirstName"",
    ""_"".""Title"" as ""Title"",
    ""_"".""Address"" as ""Address"",
    ""_"".""HiredAt"" as ""HiredAt"",
    ""_"".""Birthday"" as ""Birthday"",
    ""_"".""HomePhone"" as ""HomePhone"",
    ""_"".""Extension"" as ""Extension"",
    ""_"".""ReportsTo"" as ""ReportsTo"",
    ""_"".""Notes"" as ""Notes"",
    ""_"".""Territories"" as ""Territories"",
    ""_"".""json()"" as ""json()"",
    replace(""_"".""Address"", '{""Line1"":""7 Houndstooth Rd."",""Line2"":null,""City"":""London"",""Region"":null,""PostalCode"":""WG2 7LT"",""Country"":""UK"",""Location"":null}', '{}') as ""t0_0""
from
(
    select ""_"".""id()"",
        ""_"".""LastName"",
        ""_"".""FirstName"",
        ""_"".""Title"",
        ""_"".""Address"",
        ""_"".""HiredAt"",
        ""_"".""Birthday"",
        ""_"".""HomePhone"",
        ""_"".""Extension"",
        ""_"".""ReportsTo"",
        ""_"".""Notes"",
        ""_"".""Territories"",
        ""_"".""json()""
    from
    (
        select ""_"".""id()"",
            ""_"".""LastName"",
            ""_"".""FirstName"",
            ""_"".""Title"",
            ""_"".""Address"",
            ""_"".""HiredAt"",
            ""_"".""Birthday"",
            ""_"".""HomePhone"",
            ""_"".""Extension"",
            ""_"".""ReportsTo"",
            ""_"".""Notes"",
            ""_"".""Territories"",
            ""_"".""json()""
        from
        (
            select ""_"".""id()"",
                ""_"".""LastName"",
                ""_"".""FirstName"",
                ""_"".""Title"",
                ""_"".""Address"",
                ""_"".""HiredAt"",
                ""_"".""Birthday"",
                ""_"".""HomePhone"",
                ""_"".""Extension"",
                ""_"".""ReportsTo"",
                ""_"".""Notes"",
                ""_"".""Territories"",
                ""_"".""json()""
            from
            (
                from Employees where startsWith(LastName, 'D')
            ) ""_""
            where ""_"".""FirstName"" = 'Anne' and ""_"".""FirstName"" is not null
        ) ""_""
        where (""_"".""FirstName"" = 'Anne' and ""_"".""FirstName"" is not null) and (""_"".""LastName"" = 'Dodsworth' and ""_"".""LastName"" is not null)
    ) ""_""
    where ""_"".""FirstName"" = 'Anne' and ""_"".""FirstName"" is not null
) ""_""
limit 1000";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                // queryWithSingleReplace

                var result = await Act(store, query, Server);

                DataRowCollection rows = result.Rows;

                Assert.Equal(1, rows.Count);

                var updatedAddress = result
                    .AsEnumerable()
                    .Select(x => x.Field<string>("t0_0"))
                    .First();
                
                Assert.Equal("{}", updatedAddress);

            }
        }

        [Fact]
        public async Task WillGenerateSchemaAndReturnEmptyResponseEvenIfQueryReturnsNoResults()
        {
            const string queryWithSingleWhereCondition = @"select ""_"".""id()"",
    ""_"".""LastName"",
    ""_"".""FirstName"",
    ""_"".""Title"",
    ""_"".""Address"",
    ""_"".""HiredAt"",
    ""_"".""Birthday"",
    ""_"".""HomePhone"",
    ""_"".""Extension"",
    ""_"".""ReportsTo"",
    ""_"".""Notes"",
    ""_"".""Territories"",
    ""_"".""json()""
from 
(
    select ""_"".""id()"",
        ""_"".""LastName"",
        ""_"".""FirstName"",
        ""_"".""Title"",
        ""_"".""Address"",
        ""_"".""HiredAt"",
        ""_"".""Birthday"",
        ""_"".""HomePhone"",
        ""_"".""Extension"",
        ""_"".""ReportsTo"",
        ""_"".""Notes"",
        ""_"".""Territories"",
        ""_"".""json()""
    from 
    (
        from Employees where startsWith(LastName, 'D')
    ) ""_""
    where (""_"".""FirstName"" <> 'Anne' or ""_"".""FirstName"" is null) and (""_"".""FirstName"" <> 'Nancy' or ""_"".""FirstName"" is null)
) ""_""
where (""_"".""FirstName"" <> 'Anne' or ""_"".""FirstName"" is null) and (""_"".""FirstName"" <> 'Nancy' or ""_"".""FirstName"" is null)
limit 1000";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                // queryWithSingleReplace

                var result = await Act(store, queryWithSingleWhereCondition, Server);

                DataRowCollection rows = result.Rows;

                Assert.Equal(0, rows.Count);
            }
        }
    }
}
