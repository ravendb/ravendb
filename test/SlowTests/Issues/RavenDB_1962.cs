using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_1962 : RavenTestBase
    {
        private const int Cntr = 5;

        [Fact]
        public async Task CanExecuteLazyLoadsInAsyncSession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await StoreDataAsync(store, session);

                    var userFetchTasks = LazyLoadAsync(store, session);
                    var i = 1;
                    foreach (var lazy in userFetchTasks)
                    {
                        var user = await lazy.Value;
                        Assert.Equal(user.Name, "Test User #" + i);
                        i++;
                    }
                }
            }
        }

        [Fact]
        public async Task CanExecuteLazyLoadsInAsyncSession_CheckSingleCall()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await StoreDataAsync(store, session);
                }
                using (var session = store.OpenAsyncSession())
                {

                    LazyLoadAsync(store, session);

                    var requestTimes = await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(requestTimes.TotalClientDuration);
                    Assert.NotNull(requestTimes.TotalServerDuration);
                    Assert.Equal(Cntr, requestTimes.DurationBreakdown.Count);
                }
            }
        }

        [Fact]
        public async Task CanExecuteLazyQueriesInAsyncSession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await StoreDataAsync(store, session);
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var q1 = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "Test User #1").LazilyAsync();

                    var q2 = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "Test User #3").LazilyAsync();

                    var requestTimes = await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();
                    Assert.NotNull(requestTimes.TotalClientDuration);
                    Assert.NotNull(requestTimes.TotalServerDuration);
                    Assert.Equal(2, requestTimes.DurationBreakdown.Count);

                    Assert.Equal(1, (await q1.Value).Count());
                    Assert.Equal(1, (await q2.Value).Count());
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        public async Task StoreDataAsync(DocumentStore store, IAsyncDocumentSession session)
        {
            for (var i = 1; i <= Cntr; i++)
            {
                await session.StoreAsync(new User { Name = "Test User #" + i }, "users/" + i);
            }
            await session.SaveChangesAsync();
        }

        public List<Lazy<Task<User>>> LazyLoadAsync(DocumentStore store, IAsyncDocumentSession session)
        {
            var listTasks = new List<Lazy<Task<User>>>();
            for (var i = 1; i <= Cntr; i++)
            {
                var userFetchTask = session.Advanced.Lazily.LoadAsync<User>("users/" + i);

                listTasks.Add(userFetchTask);
            }
            return listTasks;
        }

    }
}
