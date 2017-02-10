using System.Linq;
using FastTests;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Client.Operations.Databases;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class CreateIndexesOnRemoteServer : RavenNewTestBase
    {
        [Fact]
        public void CanCreateIndex()
        {
            DoNotReuseServer();
            const string name = "CreateIndexesOnRemoteServer_1";
            var doc = MultiDatabase.CreateDatabaseDocument(name);

            using (var store = new DocumentStore { Url = UseFiddler(Server.WebUrls[0]), DefaultDatabase = name })
            {
                store.Initialize();

                store.Admin.Send(new CreateDatabaseOperation(doc));

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