// -----------------------------------------------------------------------
//  <copyright file="OldIndexRunWhileNewIndexesAreRunning.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexing;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class OldIndexRunWhileNewIndexesAreRunning : RavenTestBase
    {
        [Fact]
        public async Task OneBigSave()
        {
            using (var store = await GetDocumentStore(modifyDatabaseDocument: document => document.Settings[RavenConfiguration.GetKey(x => x.Indexing.MaxNumberOfDocumentsToFetchForMap)] = "128"))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1024 * 6; i++)
                    {
                        session.Store(new User { });
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var usersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count();

                    Assert.Equal(1024 * 6, usersCount);
                }
            }
        }

        [Fact]
        public async Task ShouldWork()
        {
            using (var store = await GetDocumentStore(modifyDatabaseDocument: document => document.Settings[RavenConfiguration.GetKey(x => x.Indexing.MaxNumberOfDocumentsToFetchForMap)] = "128"))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1024 * 6; i++)
                    {
                        session.Store(new User { });
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var usersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count();

                    Assert.Equal(1024 * 6, usersCount);
                }

                store.DatabaseCommands.PutIndex("test", new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.Name }" }
                });

                using (var session = store.OpenSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = 1000;
                    while (true) // we have to wait until we _start_ indexing
                    {
                        var objects = session.Advanced.DocumentQuery<object>("test").Take(1).ToList();
                        if (objects.Count > 0)
                            break;
                        Thread.Sleep(10);
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User { });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {

                    var usersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count();

                    Assert.Equal(1024 * 6 + 1, usersCount);
                }
            }
        }
    }
}
