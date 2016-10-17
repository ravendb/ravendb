// -----------------------------------------------------------------------
//  <copyright file="RavenDB-5303.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_5303 : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void can_load_documents_with_transformer_with_load_document_documents_starting_with(string storageType)
        {
            using (var server = GetNewServer(requestedStorage: storageType))
            using (var store = NewRemoteDocumentStore(true, ravenDbServer: server))
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
                    var docs = session.Advanced.LoadStartingWith<TestDocumentTransformer, TestDocumentTransformer.Output>("test/", start: 0, pageSize: 1024).ToList();
                    Assert.Equal(2, docs.Count);
                    Assert.True(docs[0].ValueFormatted.StartsWith("Formatted Value is {\r\n  \"Value\": 1,\r\n"));
                    Assert.True(docs[1].ValueFormatted.StartsWith("Formatted Value is {\r\n  \"Value\": 2,\r\n"));
                }

                using (var session = store.OpenSession())
                {
                    var docs = session.Advanced.LoadStartingWith<TestDocumentTransformer2, TestDocumentTransformer2.Output>("test/", start: 0, pageSize: 1024).ToList();
                    Assert.Equal(2, docs.Count);
                    Assert.True(docs[0].ValueFormatted.StartsWith("Formatted Value is {\r\n  \"Value\": 1,\r\n"));
                    Assert.True(docs[1].ValueFormatted.StartsWith("Formatted Value is {\r\n  \"Value\": 2,\r\n"));
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_load_documents_with_transformer_with_load_document_streaming(string storageType)
        {
            using (var server = GetNewServer(requestedStorage: storageType))
            using (var store = NewRemoteDocumentStore(true, ravenDbServer: server))
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

                using (var enumerator = store.DatabaseCommands.StreamDocs(Etag.Empty, transformer: transformer.TransformerName))
                {
                    var count = 0;
                    while (enumerator.MoveNext())
                    {
                        var result = enumerator.Current;
                        switch (count)
                        {
                            case 0:
                                Assert.True(result.Value<string>("ValueFormatted").StartsWith("Formatted Value is {\r\n  \"Value\": 1,\r\n"));
                                break;
                            case 1:
                                Assert.True(result.Value<string>("ValueFormatted").StartsWith("Formatted Value is {\r\n  \"Value\": 2,\r\n"));
                                break;
                        }
                        count++;
                    }

                    Assert.Equal(2, count);
                }

                using (var enumerator = store.DatabaseCommands.StreamDocs(Etag.Empty, transformer: transformer2.TransformerName))
                {
                    var count = 0;
                    while (enumerator.MoveNext())
                    {
                        var result = enumerator.Current;
                        switch (count)
                        {
                            case 0:
                                Assert.True(result.Value<string>("ValueFormatted").StartsWith("Formatted Value is {\r\n  \"Value\": 1,\r\n"));
                                break;
                            case 1:
                                Assert.True(result.Value<string>("ValueFormatted").StartsWith("Formatted Value is {\r\n  \"Value\": 2,\r\n"));
                                break;
                        }
                        count++;
                    }

                    Assert.Equal(2, count);
                }
            }
        }

        public class TestDocument
        {
            public string Id { get; set; }
            public int Value { get; set; }
        }

        public class OtherDocument
        {
            public string Id { get; set; }
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
                    let otherDoc = LoadDocument<OtherDocument>("foo")
                    select new Output
                    {
                        ValueFormatted = string.Format("Formatted Value is {0}", result)
                    };
            }
        }

        public class TestDocumentTransformer2 : AbstractTransformerCreationTask<TestDocument>
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
                        ValueFormatted = string.Format("Formatted Value is {0}", result)
                    };
            }
        }
    }
}
