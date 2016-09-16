using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class TestIndexAndTransforSubcollections : RavenTestBase
    {
        [Fact]
        public void CanTransformMultipleIndexResult()
        {
            using (var store = GetDocumentStore())
            {
                new IndexSubCollection().Execute(store);
                new IndexSubCollectionResultTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ItemWithSubCollection
                    {
                        Name = "MyAggregate",
                        SubCollection = new[]
                        {
                            "SubItem1",
                            "SubItem2",
                            "SubItem3"
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<IndexSubCollectionResult, IndexSubCollection>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow()
                            .SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(true))
                        .TransformWith<IndexSubCollectionResultTransformer, IndexSubCollectionProjection>()
                        .ToList();

                    Assert.Equal(3, result.Count);
                    Assert.True(result.Any(x => x.TransformedSubItem == "transformed_SubItem1"));
                    Assert.True(result.Any(x => x.TransformedSubItem == "transformed_SubItem2"));
                    Assert.True(result.Any(x => x.TransformedSubItem == "transformed_SubItem3"));
                }
            }
        }

        private class ItemWithSubCollection
        {
            public string Name { get; set; }
            public IList<string> SubCollection { get; set; }
        }

        private class IndexSubCollectionResult
        {
            public string Name { get; set; }
            public string SubItem { get; set; }
        }

        private class IndexSubCollectionProjection
        {
            public string TransformedSubItem { get; set; }
        }

        private class IndexSubCollection : AbstractIndexCreationTask<ItemWithSubCollection>
        {
            public IndexSubCollection()
            {
                Map = docs => from doc in docs
                    from subItem in doc.SubCollection
                    select new
                    {
                        doc.Name,
                        SubItem = subItem
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class IndexSubCollectionResultTransformer : AbstractTransformerCreationTask<IndexSubCollectionResult>
        {
            public IndexSubCollectionResultTransformer()
            {
                TransformResults = results => from result in results
                    select new
                    {
                        TransformedSubItem = "transformed_" + result.SubItem
                    };
            }
        }
    }
}
