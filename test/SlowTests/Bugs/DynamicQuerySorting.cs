using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Bugs
{
    public class DynamicQuerySorting : RavenTestBase
    {
        public class GameServer
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void ShouldNotSortStringAsLong()
        {
            using (var store = GetDocumentStore())
            {
                QueryStatistics stats;
                using (var session = store.OpenSession())
                {
                    session.Query<GameServer>()
                        .Statistics(out stats)
                        .OrderBy(x => x.Name)
                        .ToList();
                }
                var indexDefinition = store.Admin.Send(new GetIndexOperation(stats.IndexName));              
                Assert.Equal(SortOptions.String, indexDefinition.Fields["Name"].Sort);
            }
        }

        [Fact]
        public void ShouldNotSortStringAsLongAfterRestart()
        {
            using (var store = GetDocumentStore())
            {
                QueryStatistics stats;
                using (var session = store.OpenSession())
                {
                    session.Query<GameServer>()
                        .Statistics(out stats)
                        .OrderBy(x => x.Name)
                        .ToList();
                }

                var indexDefinition = store.Admin.Send(new GetIndexOperation(stats.IndexName));
                Assert.Equal(SortOptions.String, indexDefinition.Fields["Name"].Sort);
            }
        }

        [Fact]
        public void ShouldSelectIndexWhenNoSortingSpecified()
        {
            using (var store = GetDocumentStore())
            {
                QueryStatistics stats;
                using (var session = store.OpenSession())
                {
                    session.Query<GameServer>()
                        .Statistics(out stats)
                        .OrderBy(x => x.Name)
                        .ToList();
                }

                var indexQuery = new IndexQuery()
                {
                   // SortedFields = new[]
                   //{
                   //     new SortedField("Name"),
                   // }
                };
                
                var indexName = store.Commands().Query("dynamic/GameServers", indexQuery).IndexName;
                Assert.Equal(stats.IndexName, indexName);
            }
        }
    }
}
