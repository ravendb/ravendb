// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4241.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4241 : RavenTestBase
    {
        public RavenDB_4241(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void passing_duplicate_document_ids_to_Advanced_Lazily_Load_should_not_result_in_documents_failing_to_load()
        {
            using (var documentStore = GetDocumentStore())
            {
                const string documentId = "TestDocuments/1";
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new TestDocument { Id = documentId, Value = 1 });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    session.Advanced.Lazily.Load<TestDocument>(new[] { documentId, documentId });
                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                    var doc = session.Load<TestDocument>(documentId);

                    Assert.NotNull(doc);
                    Assert.Equal(1, doc.Value);
                }

                using (var session = documentStore.OpenSession())
                {
                    var lazyLoad = session.Advanced.Lazily.Load<TestDocument>(new[] { documentId, documentId });

                    var docs = lazyLoad.Value;

                    Assert.Equal(1, docs.Count);
                    Assert.NotNull(docs[documentId]);
                }

                using (var session = documentStore.OpenSession())
                {
                    var lazyLoad = session.Advanced.Lazily.Load<TestDocument>(new[] { documentId, documentId, documentId });

                    var docs = lazyLoad.Value;

                    Assert.Equal(1, docs.Count);
                    Assert.NotNull(docs[documentId]);
                }

                using (var session = documentStore.OpenSession())
                {
                    var lazyLoad = session.Advanced.Lazily.Load<TestDocument>(new[] { documentId, "items/123", documentId });

                    var docs = lazyLoad.Value;

                    Assert.Equal(2, docs.Count);
                    Assert.NotNull(docs[documentId]);
                    Assert.Null(docs["items/123"]);
                }
            }
        }

        [Fact]
        public void passing_duplicate_document_ids_to_Load_with_Include_should_not_throw()
        {
            using (var documentStore = GetDocumentStore())
            {
                const string documentId = "TestDocuments/1";
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new TestDocument { Id = documentId, Value = 1 });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var docs = session.Include("foo").Load<TestDocument>(new[] { documentId, documentId });

                    Assert.Equal(1, docs.Count);
                }

                using (var session = documentStore.OpenSession())
                {
                    var docs = session.Include("foo").Load<TestDocument>(new[] { documentId, "items/123", documentId });

                    Assert.Equal(2, docs.Count);
                    Assert.NotNull(docs[documentId]);
                    Assert.Null(docs["items/123"]);
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
