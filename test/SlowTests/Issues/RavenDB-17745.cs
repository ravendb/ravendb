using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Util;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17745 : RavenTestBase
    {
        public RavenDB_17745(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task BulkInsertWithDelay()
        {
            using (var store = GetDocumentStore())
            {
                StreamWithTimeout.DefaultWriteTimeout = TimeSpan.FromSeconds(20);
                StreamWithTimeout.DefaultReadTimeout = TimeSpan.FromSeconds(20);
                var bulk = store.BulkInsert();

                await Task.Delay(StreamWithTimeout.DefaultWriteTimeout + TimeSpan.FromSeconds(5));
                bulk.Store(new User { Name = "Daniel" }, "users/1");
                bulk.Store(new User { Name = "Yael" }, "users/2");

                await Task.Delay(StreamWithTimeout.DefaultWriteTimeout + TimeSpan.FromSeconds(5));
                bulk.Store(new User { Name = "Ido" }, "users/3");
                await Task.Delay(StreamWithTimeout.DefaultWriteTimeout + TimeSpan.FromSeconds(5));
                bulk.Dispose();

                //WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("Daniel", user.Name);

                    user = session.Load<User>("users/2");
                    Assert.NotNull(user);
                    Assert.Equal("Yael", user.Name);

                    user = session.Load<User>("users/3");
                    Assert.NotNull(user);
                    Assert.Equal("Ido", user.Name);
                }
            }
        }
        [Fact]
        public async Task StartStoreInTheMiddleOfAnHeartbeat()
        {
            using (var store = GetDocumentStore())
            {
                StreamWithTimeout.DefaultWriteTimeout = TimeSpan.FromSeconds(20);
                var bulk = store.BulkInsert();

                bulk.ForTestingPurposesOnly().startStore = () =>
                {
                    bulk.StoreAsync(new User { Name = "Daniel" }, "users/1");
                };

                await Task.Delay(StreamWithTimeout.DefaultWriteTimeout + TimeSpan.FromSeconds(5));

                bulk.Dispose();

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("Daniel", user.Name);
                }
            }
        }
    }
}
