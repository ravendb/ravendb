using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using SlowTests.Core.Utils.Entities;
using SlowTests.Core.Utils.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22750 : RavenTestBase
{
    public RavenDB_22750(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying)]
    public async Task IndexTimestamp_And_LastQueryTime_Needs_To_Have_DateTimeKind_Specified()
    {
        using (var store = GetDocumentStore())
        {
            await new Companies_ByEmployeeLastName().ExecuteAsync(store);

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.Query<Company, Companies_ByEmployeeLastName>()
                    .Statistics(out var stats)
                    .ToListAsync();

                Assert.Equal(DateTimeKind.Utc, stats.IndexTimestamp.Kind);
                Assert.Equal(DateTimeKind.Utc, stats.LastQueryTime.Kind);
            }
        }
    }
}
