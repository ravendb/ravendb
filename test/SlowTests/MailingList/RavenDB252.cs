using System.Linq;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Client.Indexing;
using Raven.Json.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class RavenDB252 : RavenTestBase
    {
        [Fact(Skip = "http://issues.hibernatingrhinos.com/issue/RavenDB-5204")]
        public void EntityNameIsNowCaseInsensitive()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put("a", null, new RavenJObject
                {
                    {"FirstName", "Oren"}
                }, new RavenJObject
                {
                    {Constants.Headers.RavenEntityName, "Users"}
                });

                store.DatabaseCommands.Put("b", null, new RavenJObject
                {
                    {"FirstName", "Ayende"}
                }, new RavenJObject
                {
                    {Constants.Headers.RavenEntityName, "users"}
                });

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<User>().Where(x => x.FirstName == "Oren"));

                    Assert.NotEmpty(session.Query<User>().Where(x => x.FirstName == "Ayende"));
                }
            }
        }

        [Fact(Skip = "http://issues.hibernatingrhinos.com/issue/RavenDB-5204")]
        public void EntityNameIsNowCaseInsensitive_Method()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put("a", null, new RavenJObject
                {
                    {"FirstName", "Oren"}
                }, new RavenJObject
                {
                    {Constants.Headers.RavenEntityName, "Users"}
                });

                store.DatabaseCommands.Put("b", null, new RavenJObject
                {
                    {"FirstName", "Ayende"}
                }, new RavenJObject
                {
                    {Constants.Headers.RavenEntityName, "users"}
                });

                WaitForIndexing(store);

                store.DatabaseCommands.PutIndex("UsersByName", new IndexDefinition
                {
                    Maps = { "docs.users.Select(x=>new {x.FirstName })" }
                });

                WaitForIndexing(store);

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
