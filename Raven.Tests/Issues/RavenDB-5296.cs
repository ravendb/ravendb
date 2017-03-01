using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mono.CSharp;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class UsersByName : AbstractIndexCreationTask
    {
        public override string IndexName
        {
            get { return "Users/ByName"; }
        }

        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Map = @"from user in docs.Users
                            select new
                            {
                                user.Name
                            }"
            };
        }
    }

    public class RavenDB_5296 : RavenTestBase
    {
        [Fact]
        public void QueryLuceneMinusOperator()
        {
            using (var store = NewDocumentStore())
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

                var query = store.DatabaseCommands.Query("Users/ByName", new IndexQuery { Query = "Name:* AND -Name:First" });
                Assert.Equal(query.TotalResults, 1);
            }
        }

        [Fact]
        public void QueryLuceneNotOperator()
        {
            using (var store = NewDocumentStore())
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

                var query = store.DatabaseCommands.Query("Users/ByName", new IndexQuery { Query = "Name:* AND NOT Name:Second" });
                Assert.Equal(query.TotalResults, 1);
            }
        }
    }
}
