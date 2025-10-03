using System;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13488 : RavenTestBase
    {
        public RavenDB_13488(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ClearShouldClearDefferedCommandsAsWell(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string firstCV = null;
                using (var session = store.OpenSession())
                {
                    User firstUser = new User
                    {
                        Name = "UserWithoutAttachment"
                    };
                    session.Store(firstUser, "users/1");
                    session.SaveChanges();
                    firstCV = session.Advanced.GetChangeVectorFor(firstUser);
                    firstUser.Age = 55;
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;
                    User concurrentUser = new User
                    {
                        Name = "UserWithAttachment"
                    };
                    session.Store(concurrentUser, firstCV, "users/1");
                    session.Advanced.Attachments.Store(concurrentUser, "myPic", new MemoryStream(new byte[] { 1, 2, 3, 4 }));

                    Assert.Throws<ConcurrencyException>(session.SaveChanges);
                    Assert.Equal(1, (session as InMemoryDocumentSessionOperations).DeferredCommandsCount);

                    session.Advanced.Clear();
                    Assert.Equal(0, (session as InMemoryDocumentSessionOperations).DeferredCommandsCount);

                    session.Store(concurrentUser, null, "users/1");
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ClearShouldClearLazyRequestsAsWell(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string firstCV = null;
                using (var session = store.OpenSession())
                {
                    User firstUser = new User
                    {
                        Name = "UserWithoutAttachment"
                    };
                    session.Store(firstUser, "users/1");
                    session.SaveChanges();
                    firstCV = session.Advanced.GetChangeVectorFor(firstUser);
                    firstUser.Age = 55;
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var sessionRequests = session.Advanced.NumberOfRequests;
                    var query = session.Query<User>().Lazily();
                    Assert.Equal(sessionRequests, session.Advanced.NumberOfRequests);
                    session.Query<User>().ToList();

                    session.Advanced.Clear();

                    Assert.Equal(sessionRequests + 1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ClearShouldClearClusterOperationsAsWell(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("foo", "bar");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue("foo2", 2);
                    Assert.Throws<InvalidOperationException>(() => session.Advanced.ClusterTransaction.CreateCompareExchangeValue("foo", "bar"));
                    Assert.Throws<InvalidOperationException>(() => session.Advanced.ClusterTransaction.CreateCompareExchangeValue("foo2", 2));
                    session.Advanced.Clear();
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("foo", "bar"); // will throw if already stored
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("foo2", 2);// will throw if already deleted
                }
            }
        }
    }
}
