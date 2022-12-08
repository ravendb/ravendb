using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
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
            const string timeSeriesName = "Count";

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
                session.TimeSeriesFor(id, timeSeriesName).Append(DateTime.Today, 3);
                session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(5));
                await Assert.ThrowsAsync<RavenTimeoutException>(async () => await session.SaveChangesAsync());
            }
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
