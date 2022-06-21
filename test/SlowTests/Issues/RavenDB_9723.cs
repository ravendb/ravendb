using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9723 : RavenTestBase
    {
        public RavenDB_9723(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_wait_until_index_processes_tombstone()
        {
            using (var store = GetDocumentStore())
            {
                new RavenDB_9721.Users_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "foo"
                    }, "users/1");

                    session.Advanced.WaitForIndexesAfterSaveChanges();

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");

                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(1), throwOnTimeout: true);

                    var ex = Assert.Throws<RavenTimeoutException>(() => session.SaveChanges()); // expected since indexing is stopped

                    Assert.Contains("TimeoutException", ex.Message);
                }

                store.Maintenance.Send(new StartIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "foo"
                    }, "users/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");

                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(60), throwOnTimeout: true);

                    session.SaveChanges();

                    var users = session.Query<User, RavenDB_9721.Users_ByName>().ToList();

                    Assert.Empty(users);
                }
            }
        }
    }
}
