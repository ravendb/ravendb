using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21951 : RavenTestBase
{
    public RavenDB_21951(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void JavaScriptIndexWithBoostOnIndexEntryShouldWork(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var o1 = new Order() { Name = "CoolName", Freight = 21.37 };
                var o2 = new Order() { Name = "SomeOtherName", Freight = 1 };
                var o3 = new Order() { Name = "CoolName", Freight = 10 };

                session.Store(o1);
                session.Store(o2);
                session.Store(o3);

                session.SaveChanges();

                var index = new DummyIndex();

                index.Execute(store);

                Indexes.WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                var result = session.Query<Order, DummyIndex>().Where(x => x.Name == "CoolName").OrderByScore().ToList();

                Assert.Equal(2, result.Count);

                var metadata1 = session.Advanced.GetMetadataFor(result[0]);
                var metadata2 = session.Advanced.GetMetadataFor(result[1]);

                var score1 = metadata1[Constants.Documents.Metadata.IndexScore];
                var score2 = metadata2[Constants.Documents.Metadata.IndexScore];

                Assert.Equal(20, (long)score1);
                Assert.Equal(10, (long)score2);
            }
        }
    }

    private class DummyIndex : AbstractJavaScriptIndexCreationTask
    {
        public DummyIndex()
        {
            Maps = new HashSet<string>()
            {
               @"map('orders', function(order) {
                    return boost({
                        Name: order.Name
                    }, order.Freight)
                })"
            };
        }
    }

    private class Order
    {
        public string Name { get; set; }
        public double Freight { get; set; }
    }
}
