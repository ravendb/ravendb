using System;
using System.Linq;
using FastTests;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Client.Operations.Databases.Indexes;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class CanIndexAllDocsWhenThereAreMoreDocsThanTheBatchSize : RavenNewTestBase
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

        private readonly Action<DatabaseDocument> _modifyMapTimeout = doc =>
        {
            doc.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "0";
        };

        [Fact]
        public void WillIndexAllWhenCreatingIndex()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: _modifyMapTimeout))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 15; i++)
                    {
                        session.Store(new User { Name = "1" });
                    }
                    session.SaveChanges();
                }

                store.Admin.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Maps = { "from doc in docs select new { doc.Name}" },
                        Name = "test"
                    }}));

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>("test").Customize(x => x.WaitForNonStaleResults()).ToArray();

                    Assert.Equal(15, users.Length);
                }
            }
        }

        [Fact]
        public void WillIndexAllAfterCreatingIndex()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: _modifyMapTimeout))
            {
                store.Admin.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Maps = { "from doc in docs select new { doc.Name}" },
                        Name = "test"
                    }}));

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
