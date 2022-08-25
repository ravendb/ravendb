using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Json;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17157 : ClusterTestBase
    {
        public RavenDB_17157(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void OnSessionCreated_WaitForIndexesAfterSaveChanges_Test()
        {
            using (var store = GetDocumentStore())
            {
                store.OnSessionCreated += (sender, sessionCreatedEventArgs) =>
                {
                    sessionCreatedEventArgs.Session.WaitForIndexesAfterSaveChanges();
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Foo" });
                    session.Store(new User { Name = "Bar" });
                    session.SaveChanges();

                    List<User> results = session.Query<User>().Where(x => x.Name.StartsWith("Foo")).ToList();

                    session.Store(new User { Name = "FooBar" });
                    session.SaveChanges();

                    results = session.Query<User>().Where(x => x.Name.StartsWith("Foo")).ToList();
                }
            }
        }

        [Fact]
        public async Task OnSessionCreated_WaitForReplicationAfterSaveChanges_Test()
        {
            var db = "DatabaseNodes";
            var (_, leader) = await CreateRaftCluster(3);
            await CreateDatabaseInCluster(db, 3, leader.WebUrl);
            using (var store = new DocumentStore
                   {
                       Database = db,
                       Urls = new[] { leader.WebUrl }
                   }.Initialize())
            {

                store.OnSessionCreated += (sender, sessionCreatedEventArgs) =>
                {
                    sessionCreatedEventArgs.Session.WaitForReplicationAfterSaveChanges();
                };
                
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Foo" });
                    session.Store(new User { Name = "Bar" });
                    session.SaveChanges();

                    List<User> results = session.Query<User>().Where(x => x.Name.StartsWith("Foo")).ToList();

                    session.Store(new User { Name = "FooBar" });
                    session.SaveChanges();

                    results = session.Query<User>().Where(x => x.Name.StartsWith("Foo")).ToList();
                }
            }
        }
    }
}

