using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class RavenDB252 : RavenTestBase
    {
        public RavenDB252(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void EntityNameIsNowCaseInsensitive(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    commands.Put("a", null, new
                    {
                        FirstName = "Oren"
                    }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Users"}
                    });

                    commands.Put("b", null, new
                    {
                        FirstName = "Ayende"
                    }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "users"}
                    });
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<User>().Where(x => x.FirstName == "Oren"));

                    Assert.NotEmpty(session.Query<User>().Where(x => x.FirstName == "Ayende"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void EntityNameIsNowCaseInsensitive_Method(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    commands.Put("a", null, new
                    {
                        FirstName = "Oren"
                    }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Users"}
                    });

                    commands.Put("b", null, new
                    {
                        FirstName = "Ayende"
                    }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "users"}
                    });
                }

                Indexes.WaitForIndexing(store);

                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Name = "UsersByName",
                    Maps = { "docs.users.Select(x=>new {x.FirstName })" }
                }}));

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<User>("UsersByName").Customize(x => x.WaitForNonStaleResults()).Where(x => x.FirstName == "Oren"));

                    Assert.NotEmpty(session.Query<User>("UsersByName").Where(x => x.FirstName == "Ayende"));
                }
            }
        }

        private class User
        {
            public string Id { get; set; }

            public string FirstName { get; set; }
        }
    }
}
