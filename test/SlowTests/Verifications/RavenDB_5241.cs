using FastTests;
using Xunit;

namespace SlowTests.Verifications
{
    public class RavenDB_5241 : RavenTestBase
    {
        [Fact]
        public void loading_documents_with_transformer_duplicate_ids()
        {
            using (var store = GetDocumentStore())
            {

                var document1Id = "TestDocuments/1";
                var document2Id = "TestDocuments/2";
                using (var session = store.OpenSession())
                {
                    var existing1 = session.Load<TestDocument>(document1Id);
                    if (existing1 == null)
                        session.Store(new TestDocument { Id = document1Id, Value = 1 });

                    var existing2 = session.Load<TestDocument>(document2Id);
                    if (existing2 == null)
                        session.Store(new TestDocument { Id = document2Id, Value = 2 });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var docs = session.Load<dynamic>(new[] { document1Id, document1Id, document2Id, document1Id, document2Id });
                    Assert.Equal(2, docs.Count);
                    Assert.Equal(1, docs[document1Id].Value);
                    Assert.Equal(2, docs[document2Id].Value);
                }
            }
        }

        [Fact]
        public void loading_documents_with_transformer_duplicate_ids_and_non_existing_document()
        {
            using (var store = GetDocumentStore())
            {
                var document1Id = "TestDocuments/1";
                var document2Id = "TestDocuments/2";
                using (var session = store.OpenSession())
                {
                    var existing1 = session.Load<TestDocument>(document1Id);
                    if (existing1 == null)
                        session.Store(new TestDocument { Id = document1Id, Value = 1 });

                    var existing2 = session.Load<TestDocument>(document2Id);
                    if (existing2 == null)
                        session.Store(new TestDocument { Id = document2Id, Value = 2 });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var docs = session.Load<dynamic>(
                        new[] { document1Id, document1Id, "no_document", document2Id, document1Id, document2Id });

                    Assert.Equal(3, docs.Count);
                    Assert.Equal(1, docs[document1Id].Value);
                    Assert.Equal(2, docs[document2Id].Value);
                    Assert.Null(docs["no_document"]);
                }
            }
        }

        private class TestDocument
        {
            public string Id { get; set; }
            public int Value { get; set; }
        }
    }
}
