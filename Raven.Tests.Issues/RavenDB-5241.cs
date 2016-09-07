// -----------------------------------------------------------------------
//  <copyright file="RavenDB-5241.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Tests.Common;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5241 : RavenTest
    {
        [Fact]
        public void loading_documents_with_transformer_duplicate_ids()
        {
            using (var store = NewRemoteDocumentStore(true))
            {
                new TestDocumentTransformer().Execute(store);

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
                    var docs = session.Load<TestDocumentTransformer, TestDocumentTransformer.Output>(
                        new[] { document1Id, document1Id, document2Id, document1Id, document2Id });
                    for (int i = 0; i < docs.Length; i++)
                    {
                        Assert.NotNull(docs[i]);
                    }
                }
            }
        }

        [Fact]
        public void loading_documents_with_transformer_duplicate_ids_and_non_existing_document()
        {
            using (var store = NewRemoteDocumentStore(true))
            {
                new TestDocumentTransformer().Execute(store);

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
                    var docs = session.Load<TestDocumentTransformer, TestDocumentTransformer.Output>(
                        new[] { document1Id, document1Id, "no_document", document2Id, document1Id, document2Id });
                    for (int i = 0; i < docs.Length; i++)
                    {
                        if (i == 2)
                        {
                            Assert.Null(docs[i]);
                            continue;
                        }

                        Assert.NotNull(docs[i]);
                    }
                }
            }
        }

        public class TestDocument
        {
            public string Id { get; set; }
            public int Value { get; set; }
        }

        public class TestDocumentTransformer : AbstractTransformerCreationTask<TestDocument>
        {
            public class Output
            {
                public string ValueFormatted { get; set; }
            }

            public TestDocumentTransformer()
            {
                TransformResults = results =>
                    from result in results
                    select new Output
                    {
                        ValueFormatted = string.Format("Formatted Value is {0}", result)
                    };
            }
        }
    }
}
