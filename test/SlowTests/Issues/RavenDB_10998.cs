using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10998 : RavenTestBase
    {
        public RavenDB_10998(ITestOutputHelper output) : base(output)
        {
        }

        private class Document
        {
            public int[] Indexes { get; set; }
            public List<SubDocument> SubDocuments { get; set; }
        }

        private class SubDocument
        {
            public string Property { get; set; }
        }

        private class TheIndex : AbstractMultiMapIndexCreationTask<TheIndex.Result>
        {
            public class Result
            {
                public string ItemId { get; set; }
                public string Property { get; set; }
            }

            public TheIndex()
            {
                AddMap<Document>(docs => from doc in docs
                                         select new
                                         {
                                             ItemId = "foo",
                                             doc.SubDocuments[doc.Indexes.First()].Property
                                         });

                Reduce = results => from result in results
                                    group result by result.ItemId
                    into g
                                    select new
                                    {
                                        ItemId = g.Key,
                                        Property = g.Select(x => x.Property).First()
                                    };
            }
        }

        [Fact]
        public void IndexingWorks()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new Document
                    {
                        Indexes = new[] { 0 },
                        SubDocuments = new List<SubDocument>
                        {
                            new SubDocument
                            {
                                Property = "asdsd"
                            }
                        }
                    };

                    session.Store(doc);
                    session.SaveChanges();
                }

                new TheIndex().Execute(store);

                WaitForIndexing(store);
                WaitForUserToContinueTheTest(store);

                Assert.True(WaitForValue(() =>
                {
                    var op = new GetIndexStatisticsOperation(nameof(TheIndex));
                    return store.Maintenance.Send(op).ReduceErrors != null;
                }, true));

                var op = new GetIndexStatisticsOperation(nameof(TheIndex));
                var result = store.Maintenance.Send(op);

                Assert.Equal(0, result.ErrorsCount);
                Assert.Equal(0, result.MapErrors);
                Assert.Equal(0, result.ReduceErrors);
            }
        }
    }
}
