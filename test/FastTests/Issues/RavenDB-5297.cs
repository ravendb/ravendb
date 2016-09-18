using Raven.Client.Data;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace FastTests.Issues
{
    public class User
    {
        public string Name { get; set; }
    }

    public class UsersByName : AbstractIndexCreationTask<User>
    {
        public override string IndexName
        {
            get { return "Users/ByName"; }
        }

        public UsersByName()
        {
            Map = users => from user in users
                           select new
                           {
                               user.Name
                           };
        }
    }
    public class RavenDB_5297: RavenTestBase
    {
        [Fact]
        public void QueryLuceneMinusOperator()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                new UsersByName().Execute(store);

                store.DatabaseCommands.Put("users/1", null,
                    new RavenJObject { { "Name", "First" } },
                    new RavenJObject { { "Raven-Entity-Name", "Users" } });

                store.DatabaseCommands.Put("users/2", null,
                    new RavenJObject { { "Name", "Second" } },
                    new RavenJObject { { "Raven-Entity-Name", "Users" } });

                WaitForIndexing(store);

                var query = store.DatabaseCommands.Query("Users/ByName", new IndexQuery { Query = "Name:* -First" });
                Assert.Equal(query.TotalResults, 1);
            }
        }

        [Fact]
        public void QueryLuceneNotOperator()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                new UsersByName().Execute(store);

                store.DatabaseCommands.Put("users/1", null,
                    new RavenJObject { { "Name", "First" } },
                    new RavenJObject { { "Raven-Entity-Name", "Users" } });

                store.DatabaseCommands.Put("users/2", null,
                    new RavenJObject { { "Name", "Second" } },
                    new RavenJObject { { "Raven-Entity-Name", "Users" } });

                WaitForIndexing(store);

                var query = store.DatabaseCommands.Query("Users/ByName", new IndexQuery { Query = "Name:* -Second" });
                Assert.Equal(query.TotalResults, 1);
            }
        }
    }
}
