using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Tests.Common;
using Xunit;
using User = Raven.Tests.Common.Dto.User;

namespace Raven.SlowTests.Issues
{
    public class RavenDB_6363 : ReplicationBase
    {
        [Fact]
        public async Task FailoverShouldStillWorkAfter5MinutesAsync()
        {
            var s1 = CreateStore(databaseName: "Foo1");
            var s2 = CreateStore(databaseName: "Foo2");


            SetupReplication(s1.DatabaseCommands, s2);
            SetupReplication(s2.DatabaseCommands, s1);


            using (var store1 = new DocumentStore
            {
                Url = s1.Url,
                DefaultDatabase = s1.DefaultDatabase,
                Conventions = new DocumentConvention()
                {
                    TimeToWaitBetweenReplicationTopologyUpdates = TimeSpan.FromSeconds(10)
                }
            })
            using (var store2 = new DocumentStore
            {
                Url = s2.Url,
                DefaultDatabase = s2.DefaultDatabase,
                Conventions = new DocumentConvention()
                {
                    TimeToWaitBetweenReplicationTopologyUpdates = TimeSpan.FromSeconds(10)
                }
            })
            {
                store1.Initialize(true);
                store2.Initialize(true);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User()
                    {
                        Name = "Noam"
                    }, "users/1");

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

                await store1.GetReplicationInformerForDatabase(store1.DefaultDatabase).UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store1.AsyncDatabaseCommands);

                await store1.AsyncDatabaseCommands.GlobalAdmin.DeleteDatabaseAsync("Foo1");

                var sp = Stopwatch.StartNew();

                while (sp.ElapsedMilliseconds < 30 * 1000)
                {
                    Assert.DoesNotThrow(() =>
                    {
                        using (var session = store1.OpenAsyncSession())
                        {
                            Assert.NotNull(session.LoadAsync<User>("users/1").Result);
                        }
                    });

                    Thread.Sleep(100);

                }
            }
        }


        [Fact]
        public void FailoverShouldStillWorkAfter5MinutesSync()
        {
            var s1 = CreateStore(databaseName: "Foo1");
            var s2 = CreateStore(databaseName: "Foo2");


            SetupReplication(s1.DatabaseCommands, s2);
            SetupReplication(s2.DatabaseCommands, s1);


            using (var store1 = new DocumentStore
            {
                Url = s1.Url,
                DefaultDatabase = s1.DefaultDatabase,
                Conventions = new DocumentConvention()
                {
                    TimeToWaitBetweenReplicationTopologyUpdates = TimeSpan.FromSeconds(10)
                }
            })
            using (var store2 = new DocumentStore
            {
                Url = s2.Url,
                DefaultDatabase = s2.DefaultDatabase,
                Conventions = new DocumentConvention()
                {
                    TimeToWaitBetweenReplicationTopologyUpdates = TimeSpan.FromSeconds(10)
                }
            })
            {
                store1.Initialize(true);
                store2.Initialize(true);
                
                using (var session = store1.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Noam"
                    }, "users/1");

                    session.SaveChanges();
                }

                WaitForDocument(store2.DatabaseCommands, "users/1");

                using (var session = store1.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }

                using (var session = store2.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }

                store1.GetReplicationInformerForDatabase(store1.DefaultDatabase).UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store1.AsyncDatabaseCommands).Wait();

                store1.DatabaseCommands.GlobalAdmin.DeleteDatabase("Foo1");

                var sp = Stopwatch.StartNew();

                while (sp.ElapsedMilliseconds < 30 * 1000)
                {
                    Assert.DoesNotThrow(() =>
                    {
                        using (var session = store1.OpenSession())
                        {
                            Assert.NotNull(session.Load<User>("users/1"));
                        }
                    });
                    Thread.Sleep(100);

                }
            }
        }
    }
}
