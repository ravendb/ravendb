using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using SlowTests.Core.Utils.Entities;
using SlowTests.Core.Utils.Indexes;
using Tests.Infrastructure;
using Xunit;

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

            await Indexes.WaitForIndexingAsync(store);

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

    [RavenFact(RavenTestCategory.Querying)]
    public async Task IndexTimestamp_And_LastQueryTime_Needs_To_Have_DateTimeKind_Specified_When_Index_Did_Not_Run_Any_Batch_Yet()
    {
        using (var store = GetDocumentStore())
        {
            // stopping indexing before index deployment guarantees LastIndexingTime stays null,
            // pinning the race that makes IndexTimestamp fall back to DateTime.MinValue
            store.Maintenance.Send(new StopIndexingOperation());

            await new Companies_ByEmployeeLastName().ExecuteAsync(store);

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

    [RavenFact(RavenTestCategory.Querying)]
    public async Task IndexTimestamp_And_LastQueryTime_Needs_To_Have_DateTimeKind_Specified_For_Collection_Query()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.Query<Company>()
                    .Statistics(out var stats)
                    .ToListAsync();

                Assert.Equal(DateTimeKind.Utc, stats.IndexTimestamp.Kind);
                Assert.Equal(DateTimeKind.Utc, stats.LastQueryTime.Kind);
            }
        }
    }

    [RavenFact(RavenTestCategory.Querying)]
    public async Task Streaming_IndexTimestamp_Needs_To_Have_DateTimeKind_Specified_When_Index_Did_Not_Run_Any_Batch_Yet()
    {
        using (var store = GetDocumentStore())
        {
            store.Maintenance.Send(new StopIndexingOperation());

            await new Companies_ByEmployeeLastName().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var query = session.Query<Company, Companies_ByEmployeeLastName>();

                var reader = await session.Advanced.StreamAsync(query, out StreamQueryStatistics stats);

                while (await reader.MoveNextAsync())
                {
                }

                // StreamQueryStatistics carries only IndexTimestamp, no LastQueryTime
                Assert.Equal(DateTimeKind.Utc, stats.IndexTimestamp.Kind);
            }
        }
    }
}
