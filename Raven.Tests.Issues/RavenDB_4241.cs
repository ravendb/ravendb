// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4241.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4241 : RavenTest
    {
        [Fact]
        public void passing_duplicate_document_ids_to_Advanced_Lazily_Load_should_not_result_in_documents_failing_to_load()
        {
            using (var documentStore = NewDocumentStore())
            {
                var documentId = "TestDocuments/1";
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

                    Assert.Equal(2, docs.Length);
                    Assert.NotNull(docs[0]);
                    Assert.NotNull(docs[1]);
                    Assert.Same(docs[0], docs[1]);
                }

                using (var session = documentStore.OpenSession())
                {
                    var lazyLoad = session.Advanced.Lazily.Load<TestDocument>(new[] { documentId, documentId, documentId });

                    var docs = lazyLoad.Value;

                    Assert.Equal(3, docs.Length);
                    Assert.NotNull(docs[0]);
                    Assert.NotNull(docs[1]);
                    Assert.NotNull(docs[2]);
                    Assert.Same(docs[0], docs[1]);
                    Assert.Same(docs[1], docs[2]);
                }

                using (var session = documentStore.OpenSession())
                {
                    var lazyLoad = session.Advanced.Lazily.Load<TestDocument>(new[] { documentId, "items/123", documentId });

                    var docs = lazyLoad.Value;

                    Assert.Equal(3, docs.Length);
                    Assert.NotNull(docs[0]);
                    Assert.Null(docs[1]);
                    Assert.NotNull(docs[2]);
                    Assert.Same(docs[0], docs[2]);
                }
            }
        }

        [Fact]
        public void passing_duplicate_document_ids_to_Load_with_Include_should_not_throw()
        {
            using (var documentStore = NewDocumentStore())
            {
                var documentId = "TestDocuments/1";
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new TestDocument { Id = documentId, Value = 1 });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    TestDocument[] docs = null;

                    Assert.DoesNotThrow(() => docs = session.Include("foo").Load<TestDocument>(new[] { documentId, documentId }));

                    Assert.Equal(2, docs.Length);
                    Assert.Same(docs[0], docs[1]);
                }

                using (var session = documentStore.OpenSession())
                {
                    TestDocument[] docs = null;

                    Assert.DoesNotThrow(() => docs = session.Include("foo").Load<TestDocument>(new[] { documentId, "items/123", documentId }));

                    Assert.Equal(3, docs.Length);
                    Assert.NotNull(docs[0]);
                    Assert.Null(docs[1]);
                    Assert.NotNull(docs[2]);
                    Assert.Same(docs[0], docs[2]);
                }
            }
        }

        public class TestDocument
        {
            public string Id { get; set; }
            public int Value { get; set; }
        }
    }
}