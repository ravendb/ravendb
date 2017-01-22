using System;
using FastTests;
using Raven.Client.Indexing;
using Xunit;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Server.Config;

namespace SlowTests.Bugs.Indexing
{
    public class CanIndexAllDocsWhenThereAreMoreDocsThanTheBatchSize : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }

        Action<DatabaseDocument> modifyMapTimeout = doc =>
        {
            doc.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "0";
        };

        [Fact]
        public void WillIndexAllWhenCreatingIndex()
        {    
            using (var store = GetDocumentStore(modifyDatabaseDocument: modifyMapTimeout))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 15; i++)
                    {
                        session.Store(new User{Name="1"});
                    }
                    session.SaveChanges();
                }

                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Name}"}
                                                });

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>("test").Customize(x=>x.WaitForNonStaleResults()).ToArray();

                    Assert.Equal(15, users.Length);
                }
            }
        }

        [Fact]
        public void WillIndexAllAfterCreatingIndex()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: modifyMapTimeout))
            {
                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Name}"}
                                                });

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 15; i++)
                    {
                        session.Store(new User { Name = "1" });
                    }
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>("test").Customize(x => x.WaitForNonStaleResults()).ToArray();

                    Assert.Equal(15, users.Length);
                }
            }
        }
    }
}
