using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.SlowTests.Issues
{
    public class RavenDB_6363:ReplicationBase
    {
        [Fact]
        public async Task FailoverShouldStillWorkAfter5Minutes()
        {
            var store1 = CreateStore(databaseName:"Foo1");
            var store2 = CreateStore(databaseName: "Foo2");


            SetupReplication(store1.DatabaseCommands, store2);
            SetupReplication(store2.DatabaseCommands, store1);

            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new User()
                {
                    Name = "Noam"
                },"users/1");

                await session.SaveChangesAsync();
            }

            WaitForDocument(store2.DatabaseCommands, "users/1");


            using (var session = store1.OpenAsyncSession())
            {
                Assert.NotNull(await session.LoadAsync<User>("users/1"));
            }

            using (var session = store2.OpenAsyncSession())
            {
                Assert.NotNull(await session.LoadAsync<User>("users/1"));
            }

            await store1.AsyncDatabaseCommands.GlobalAdmin.DeleteDatabaseAsync("Foo1");


            var sp = Stopwatch.StartNew();

            while (sp.ElapsedMilliseconds < 7*60*1000)
            {
                Assert.DoesNotThrow(() =>
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        Assert.NotNull(session.LoadAsync<User>("users/1").Result);
                    }
                });
                
            }
        }
    }
}
