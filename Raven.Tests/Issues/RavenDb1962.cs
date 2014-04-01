using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Raven.Tests.Bugs;
using Raven.Client;

namespace Raven.Tests.Issues
{
    public class RavenDb1962 : RavenTest
    {
        [Fact]
        public async Task CanDisplayLazyRequestTimes_Remote()
        {
            using (var store = NewRemoteDocumentStore())
            {

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Test User #1" }, "users/1");
                    await session.StoreAsync(new User { Name = "Test User #2" }, "users/2");
                    await session.StoreAsync(new User { Name = "Test User #3" }, "users/3");
                    await session.StoreAsync(new User { Name = "Test User #4" }, "users/4");
                    await session.StoreAsync(new User { Name = "Test User #5" }, "users/5");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Lazily.LoadAsync<User>("users/1");
                    session.Advanced.Lazily.LoadAsync<User>("users/2");
                    session.Advanced.Lazily.LoadAsync<User>("users/3");
                    session.Advanced.Lazily.LoadAsync<User>("users/4");
                    session.Advanced.Lazily.LoadAsync<User>("users/5");
                    session.Advanced.AsyncDocumentQuery<User>();
                  //  session.Query<User>().Lazily();
                   
                    var requestTimes = await session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    Assert.NotNull(requestTimes.TotalClientDuration);
                    Assert.NotNull(requestTimes.TotalServerDuration);
                    Assert.Equal(5, requestTimes.DurationBreakdown.Count);

                }
            }
        }
        [Fact]
        public async Task CanDisplayLazyRequestTimes_Embedded()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Test User #1" }, "users/1");
                    await session.StoreAsync(new User { Name = "Test User #2" }, "users/2");
                    await session.SaveChangesAsync();   
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Lazily.LoadAsync<User>("users/1");
                    session.Advanced.Lazily.LoadAsync<User>("users/2");
                    session.Advanced.AsyncDocumentQuery<User>();
                   // session.Query<User>().Lazily();

                    var requestTimes = await session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    Assert.NotNull(requestTimes.TotalClientDuration);
                    Assert.NotNull(requestTimes.TotalServerDuration);
                    Assert.Equal(3, requestTimes.DurationBreakdown.Count);

                }
            }
        }

    }
}
/*
In ISyncAdvancedSessionOperation I can use Lazily to load multiple documents in one round trip.
i.e.
session.Advanced.Lazily.Load<Document>(documentId1);
session.Advanced.Lazily.Load<Document>(documentId2);
session.Advanced.Lazily.Load<Document>(documentId3);
session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

Is it possible to get Lazy loading support on IAsyncAdvancedSessionOperations?

*/