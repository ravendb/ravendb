using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19634 : RavenTestBase
{
    public RavenDB_19634(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Should_Wait_For_Time_Series_Index_Changes()
    {
        using (var store = GetDocumentStore())
        {
            const string id = "users/1";

            var index = new UsersTimeSeriesMapIndex();
            await index.ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = "Grisha"
                }, id);
                await session.SaveChangesAsync();
            }

            await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));
            
            using (var session = store.OpenAsyncSession())
            {
                session.TimeSeriesFor(id, "Count").Append(DateTime.Today, 3);
                session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(5));
                await Assert.ThrowsAsync<RavenTimeoutException>(async () => await session.SaveChangesAsync());
            }
        }
    }

    [Fact]
    public async Task Should_Wait_For_Time_Series_Copy_Index_Changes()
    {
        using (var store = GetDocumentStore())
        {
            const string id = "users/1";
            const string id2 = "users/2";

            var index = new UsersTimeSeriesMapIndex();
            await index.ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = "Grisha"
                }, id);
                await session.StoreAsync(new User
                {
                    Name = "Grisha"
                }, id2);
                session.TimeSeriesFor(id, "Count").Append(DateTime.Today, 3);
                await session.SaveChangesAsync();
            }

            await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));
            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Defer(new CopyTimeSeriesCommandData(id,
                    "Count",
                    id2,
                    "Count"));

                session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(5));
                await Assert.ThrowsAsync<RavenTimeoutException>(async () => await session.SaveChangesAsync());
            }
        }
    }

    [Fact]
    public async Task Should_Wait_For_Counters_Index_Changes()
    {
        using (var store = GetDocumentStore())
        {
            const string id = "users/1";

            var index = new UsersCountersIndex();
            await index.ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = "Grisha"
                }, id);
                await session.SaveChangesAsync();
            }

            await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));

            using (var session = store.OpenAsyncSession())
            {
                session.CountersFor(id).Increment("Count", 1);
                session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(5));
                await Assert.ThrowsAsync<RavenTimeoutException>(async () => await session.SaveChangesAsync());
            }
        }
    }

    private class UsersCountersIndex : AbstractCountersIndexCreationTask<User>
    {
        public UsersCountersIndex()
        {
            AddMapForAll(counters => from counter in counters
                select new
                {
                    Name = counter.Name,
                    Count = 1
                });
        }
    }

    private class UsersTimeSeriesMapIndex : AbstractTimeSeriesIndexCreationTask<User>
    {
        public UsersTimeSeriesMapIndex()
        {
            AddMapForAll(timeSeries =>
                from ts in timeSeries
                from entry in ts.Entries
                select new
                {
                    Name = ts.Name,
                    Count = 1
                });
        }
    }
}
