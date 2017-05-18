using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class CreateIndexesOnRemoteServer : RavenTestBase
    {
        [Fact]
        public void CanCreateIndex()
        {
            DoNotReuseServer();
            const string name = "CreateIndexesOnRemoteServer_1";
            var doc = MultiDatabase.CreateDatabaseDocument(name);

            using (var store = new DocumentStore { Urls = UseFiddler(Server.WebUrls), Database = name })
            {
                store.Initialize();

                store.Admin.Server.Send(new CreateDatabaseOperation(doc));

                new SimpleIndex().Execute(store);
                new SimpleIndex().Execute(store);
            }
        }

        private class SimpleIndex : AbstractIndexCreationTask<User>
        {
            public SimpleIndex()
            {
                Map = users => from user in users
                               select new { user.Age };
            }
        }

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
    }
}