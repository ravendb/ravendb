// -----------------------------------------------------------------------
//  <copyright file="RavenDB-5303.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5303 : RavenNewTestBase
    {
        [Fact]
        public void can_load_documents_with_transformer_with_load_document_documents_starting_with()
        {
            using (var store = GetDocumentStore())
            {
                new TestDocumentTransformer().Execute(store);
                new TestDocumentTransformer2().Execute(store);

                var document1Id = "test/1";
                var document2Id = "test/2";

                using (var session = store.OpenSession())
                {
                    session.Store(new TestDocument { Id = document1Id, Value = 1 });
                    session.Store(new TestDocument { Id = document2Id, Value = 2 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var docsWithoutTransformer = session.Advanced.LoadStartingWith<TestDocument>("test/", start: 0, pageSize: 1024).ToList();
                    Assert.Equal(2, docsWithoutTransformer.Count);
                    Assert.Equal(1, docsWithoutTransformer[0].Value);
                    Assert.Equal(2, docsWithoutTransformer[1].Value);
                }

                using (var session = store.OpenSession())
                {
                    var docs = session.Advanced.LoadStartingWith<TestDocumentTransformer, TestDocumentTransformer.Output>("test/", start: 0, pageSize: 1024).Select(x => x.Value).ToList();
                    Assert.Equal(2, docs.Count);
                    Assert.True(docs[0].ValueFormatted.StartsWith("Formatted Value is 1"));
                    Assert.True(docs[1].ValueFormatted.StartsWith("Formatted Value is 2"));
                }

                using (var session = store.OpenSession())
                {
                    var docs = session.Advanced.LoadStartingWith<TestDocumentTransformer2, TestDocumentTransformer2.Output>("test/", start: 0, pageSize: 1024).Select(x => x.Value).ToList();
                    Assert.Equal(2, docs.Count);
                    Assert.True(docs[0].ValueFormatted.StartsWith("Formatted Value is 1"));
                    Assert.True(docs[1].ValueFormatted.StartsWith("Formatted Value is 2"));
                }
            }
        }

        [Fact]
        public void can_load_documents_with_transformer_with_load_document_streaming()
        {
            using (var store = GetDocumentStore())
            {
                var transformer = new TestDocumentTransformer();
                new TestDocumentTransformer().Execute(store);
                var transformer2 = new TestDocumentTransformer2();
                new TestDocumentTransformer2().Execute(store);

                var document1Id = "test/1";
                var document2Id = "test/2";

                using (var session = store.OpenSession())
                {
                    session.Store(new TestDocument { Id = document1Id, Value = 1 });
                    session.Store(new TestDocument { Id = document2Id, Value = 2 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var docsWithoutTransformer = session.Advanced.LoadStartingWith<TestDocument>("test/", start: 0, pageSize: 1024).ToList();
                    Assert.Equal(2, docsWithoutTransformer.Count);
                    Assert.Equal(1, docsWithoutTransformer[0].Value);
                    Assert.Equal(2, docsWithoutTransformer[1].Value);
                }

                using (var session = store.OpenSession())
                {
                    using (var enumerator = session.Advanced.Stream<TestDocumentTransformer.Output>(0, transformer: transformer.TransformerName))
                    {
                        var count = 0;
                        while (enumerator.MoveNext())
                        {
                            var result = enumerator.Current.Document;
                            switch (count)
                            {
                                case 0:
                                    Assert.True(result.ValueFormatted.StartsWith("Formatted Value is 1"));
                                    break;
                                case 1:
                                    Assert.True(result.ValueFormatted.StartsWith("Formatted Value is 2"));
                                    break;
                            }
                            count++;
                        }

                        Assert.Equal(2, count);
                    }
                }

                using (var session = store.OpenSession())
                {
                    using (var enumerator = session.Advanced.Stream<TestDocumentTransformer2.Output>(0, transformer: transformer2.TransformerName))
                    {
                        var count = 0;
                        while (enumerator.MoveNext())
                        {
                            var result = enumerator.Current.Document;
                            switch (count)
                            {
                                case 0:
                                    Assert.True(result.ValueFormatted.StartsWith("Formatted Value is 1"));
                                    break;
                                case 1:
                                    Assert.True(result.ValueFormatted.StartsWith("Formatted Value is 2"));
                                    break;
                            }
                            count++;
                        }

                        Assert.Equal(2, count);
                    }
                }
            }
        }

        private class TestDocument
        {
            public string Id { get; set; }
            public int Value { get; set; }
        }

        private class OtherDocument
        {
            public string Id { get; set; }
        }

        private class TestDocumentTransformer : AbstractTransformerCreationTask<TestDocument>
        {
            public class Output
            {
                public string ValueFormatted { get; set; }
            }

            public TestDocumentTransformer()
            {
                TransformResults = results =>
                    from result in results
                    let otherDoc = LoadDocument<OtherDocument>("foo")
                    select new Output
                    {
                        ValueFormatted = string.Format("Formatted Value is {0}", result.Value)
                    };
            }
        }

        private class TestDocumentTransformer2 : AbstractTransformerCreationTask<TestDocument>
        {
            public class Output
            {
                public string ValueFormatted { get; set; }
            }

            public TestDocumentTransformer2()
            {
                TransformResults = results =>
                    from result in results
                    select new Output
                    {
                        ValueFormatted = string.Format("Formatted Value is {0}", result.Value)
                    };
            }
        }
    }
}
