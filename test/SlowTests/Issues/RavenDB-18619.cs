using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18619 : ClusterTestBase
    {
        public RavenDB_18619(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void NormsAreGeneratedCorrectlyOnWholeDocument()
        {
            using var store = GetDocumentStore();
            {
                using var session = store.OpenSession();
                var entry = new Item
                {
                    List = new[]
                    {
                        "Lorem ipsum dolor sit amet", "consectetur adipiscing elit", ", sed do eiusmod tempor incididunt ut labore ",
                        "et dolore magna aliqua. Ut enim ad ", "minim veniam, quis nostrud", " exercitation ullamco laboris nisi ut",
                        " aliquip ex ea commodo consequat. ", "Duis aute", " irure dolor in reprehenderit in voluptate velit esse cillum dolore",
                        " eu fugiat nulla pariatur. ", "Excepteur sint occaecat cupidatat", " non proident, sunt in", " culpa qui officia deserunt ",
                        "mollit anim id est laborum."
                    }
                };
                session.Store(entry, "Items/1");
                session.SaveChanges();
            }
            new NormIndex().Execute(store);
            Indexes.WaitForIndexing(store);


            using (var session = store.OpenSession())
            {
                var l = session.Query<Item, NormIndex>()
                    .Search(p => p.List, "null*", boost: 10)
                    .ToDocumentQuery()
                    .IncludeExplanations(out var explanations)
                    .ToList();
                Assert.NotEmpty(l);

                var details = explanations.GetExplanations("Items/1");
                Assert.NotNull(details);
                Assert.NotEmpty(details);
                Assert.Equal(details[0], "1 = (MATCH) ConstantScoreQuery(List:null*^10.0), product of:\n  10 = boost\n  0.1 = queryNorm\n");
            }

            using (var session = store.OpenSession())
            {
                var query = session.Query<Item, NormIndex>()
                    .Search(p => p.List, "null*", boost: 10);
                var docQuery = query
                    .ToDocumentQuery()
                    .IncludeExplanations(out var explanations2);
                var queryResults = docQuery.GetQueryResult();

                var explanations1 = queryResults.Explanations;

                Assert.NotNull(explanations1);
                Assert.NotNull(explanations2);
                Assert.Equal("1 = (MATCH) ConstantScoreQuery(List:null*^10.0), product of:\n  10 = boost\n  0.1 = queryNorm\n"
                    , explanations1["Items/1"][0]);
                Assert.Equal("1 = (MATCH) ConstantScoreQuery(List:null*^10.0), product of:\n  10 = boost\n  0.1 = queryNorm\n"
                    , explanations2.GetExplanations("Items/1")[0]);

            }
        }

        private class Item
        {
            public string[] List { get; set; }
        }

        private class NormIndex : AbstractIndexCreationTask<Item>
        {
            public NormIndex()
            {
                Map = items => from item in items
                    select new { List = item.List.ToArray() }.Boost(5);

                Index(p => p.List, FieldIndexing.Search);
            }
        }

    }
}
