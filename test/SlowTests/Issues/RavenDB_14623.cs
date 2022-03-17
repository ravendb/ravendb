using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14623 : RavenTestBase
    {
        public RavenDB_14623(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void StoreAllFields_Test()
        {
            TestDocument documentInInterest = new TestDocument
            {
                Name = "Hello world!",
                ArrayOfStrings = new string[] { "123", "234", "345" }
            };

            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new MyIndex());
                store.ExecuteIndex(new MyJSIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(documentInInterest);
                    session.Store(new TestDocument { Name = "Goodbye...", ArrayOfStrings = new string[] { "qwe", "asd", "zxc" } });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store); //If we want to query documents sometime we need to wait for the indexes to catch up

                // WaitForUserToContinueTheTest(store);//Sometimes we want to debug the test itself, this redirect us to the studio

                AssertIndex<MyIndex>(documentInInterest, store);

                AssertIndex<MyJSIndex>(documentInInterest, store);
            }
        }

        private static void AssertIndex<TIndex>(TestDocument documentInInterest, DocumentStore store)
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var session = store.OpenSession())
            {
                var query = session.Query<TestDocumentResult, TIndex>()
                    .Where(x => x.NameButDifferentName == "Hello world!")
                    .ProjectInto<TestDocumentResult>(); // we are interested in the mapped Document, our original document is actually quite big and not interesting

                // Execute DB call
                var result = query.ToList();

                // we are able to query by the NameButDifferentName property, which is indexed
                Assert.Single(query);

                var documentReturned = query.First();

                // First Issue, the NameButDifferentName is not stored, despite the __all_fields config in the index
                Assert.Equal(documentInInterest.Name, documentReturned.NameButDifferentName);

                // Second Issue, since we added a search config on this property it is stored, but with an incorrect type
                // we expect it to be array of string, however it is only string
                var typeAfterStore = documentReturned.NewPropertyName.GetType();
                Assert.Equal(typeof(string[]), typeAfterStore);
            }
        }

        private class TestDocument
        {
            public string Name { get; set; }
            public string[] ArrayOfStrings { get; set; }
        }

        private class TestDocumentResult
        {
            public string[] NewPropertyName { get; set; } // supposed to be string[] but that will result in runtime JSON parsing errors
            public string NameButDifferentName { get; set; }
        }

        private class MyIndex : AbstractIndexCreationTask<TestDocument>
        {
            public MyIndex()
            {
                Map = docs => from doc in docs
                              select new TestDocumentResult
                              {
                                  NewPropertyName = doc.ArrayOfStrings,
                                  NameButDifferentName = doc.Name
                              };

                Store("__all_fields", FieldStorage.Yes);

                Index("NewPropertyName", FieldIndexing.Search);
                Analyze("NewPropertyName", "StandardAnalyzer");
            }
        }

        private class MyJSIndex : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public object NewPropertyName { get; set; } // supposed to be string[] but that will result in runtime JSON parsing errors
                public string NameButDifferentName { get; set; }
            }

            public MyJSIndex()
            {
                Maps = new HashSet<string>()
                {
                    @"
map('TestDocuments', function (doc) {
    var res = [];

    res.push({
        NewPropertyName: doc.ArrayOfStrings,
        NameButDifferentName: doc.Name
    });

    return res;
});"
                };

                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    {
                        "__all_fields", new IndexFieldOptions
                        {
                            Storage = FieldStorage.Yes,
                        }
                    },
                    {
                        "NewPropertyName", new IndexFieldOptions
                        {
                            Analyzer = "StandardAnalyzer",
                            Indexing = FieldIndexing.Search,
                        }
                    },
                };
            }
        }
    }
}
