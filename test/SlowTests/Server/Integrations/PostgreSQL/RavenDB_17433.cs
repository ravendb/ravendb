using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
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
        public async Task QueryWithReplaceShouldWork()
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
                CreateNorthwindDatabase(store);

                // queryWithSingleReplace

                var result = await Act(store, queryWithSingleReplace, Server);

                DataRowCollection rows = result.Rows;

                Assert.Equal(2, rows.Count);

                var updatedTitles = result
                    .AsEnumerable()
                    .Select(x => x.Field<string>("t0_0"))
                    .ToList();

                Assert.Equal("Marketing Representative", updatedTitles[0]);
                Assert.Equal("Marketing Representative", updatedTitles[1]);


                // queryWithMultipleNestedReplaces

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
    }
}
