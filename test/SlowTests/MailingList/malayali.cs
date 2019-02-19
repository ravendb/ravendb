using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using System.Linq;

namespace SlowTests.MailingList
{
    public class malayali : RavenTestBase
    {
         class FanOutTestIndex : AbstractIndexCreationTask<Level2SimpleTestDocument>
        {
            public FanOutTestIndex()
            {
                this.Map = documents => from doc in documents
                                        from innerDoc in doc.SimpleTestDocumentList
                                        select new Result
                                        {
                                            InnerDocId = innerDoc.Id,
                                            Comment = innerDoc.Comment,
                                            Tags = innerDoc.Tags
                                        };

                this.StoreAllFields(FieldStorage.Yes);
            }

            public class Result
            {
                public string InnerDocId { get; set; }
                public string Comment { get; set; }
                public IEnumerable<string> Tags { get; set; }
            }
        }


        public class SimpleTestDocument
        {
            public string Id { get; set; }

            public string Comment { get; set; }

            public IEnumerable<string> Tags { get; set; }
        }

        public class Level2SimpleTestDocument
        {
            private readonly List<SimpleTestDocument> _simpleTestDocumentList = new List<SimpleTestDocument>();

            public string Id { get; set; }

            public string AssociatedPropertyId { get; set; }

            public IEnumerable<SimpleTestDocument> SimpleTestDocumentList => this._simpleTestDocumentList.AsEnumerable();

            public void AddSimpleTestDocument(SimpleTestDocument doc)
            {
                if (doc != null)
                {
                    this._simpleTestDocumentList.Add(doc);
                }
            }
        }

        [Fact]
        public void CheckIfItemExistsInList()
        {
            using (var store = GetDocumentStore())
            {
                new FanOutTestIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var testDoc = new Level2SimpleTestDocument
                    {
                        Id = "Level2SimpleTestDocuments/124",
                        AssociatedPropertyId = "AssociatedProperties/124"
                    };

                    testDoc.AddSimpleTestDocument(new SimpleTestDocument {
                        Id = "SimpleTestDocuments/124", Comment = "1234", Tags = new[] { "1234", "Noise" } });

                    session.Store(testDoc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var sq = session.Advanced.DocumentQuery<FanOutTestIndex.Result, FanOutTestIndex>()
                        .WaitForNonStaleResults()
                        .WhereEquals("Comment", "1234")
                        .WhereEquals("Tags", "1234")
                        .SelectFields<FanOutTestIndex.Result>();
                    var result = sq
                        .SingleOrDefault();
                    WaitForUserToContinueTheTest(store);
                    Assert.NotNull(result);
                    Assert.Equal("1234", result.Comment);
                }
            }
        }
    }
}
