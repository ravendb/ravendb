using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;
using System.Text;
using FastTests;
using Org.BouncyCastle.Crypto.Digests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23495(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string EmbeddingName = "embedding";

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void CanIndexEmbeddingStoredInAttachmentInJavaScriptIndex()
    {
        var v1 = new[] { 0.1f, 0.2f, 0.3f };
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();

        var doc1 = new Document { };
        session.Store(doc1);
        session.SaveChanges();
        var vectorInBase64 = Encoding.UTF8.GetBytes(Convert.ToBase64String(MemoryMarshal.Cast<float, byte>(v1)));
        using var memStream = new MemoryStream(vectorInBase64);
        session.Advanced.Attachments.Store(doc1, EmbeddingName, memStream);
        session.SaveChanges();

        var index = new JsIndexWithVector();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);

        Document doc = session.Query<Document, JsIndexWithVector>()
            .VectorSearch(f => f.WithField(s => s.Vector),
                v => v.ByEmbedding(v1))
            .FirstOrDefault();

        Assert.NotNull(doc);
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void CanIndexEmbeddingStoredInAttachmentInCsharpIndex()
    {
        var v1 = new[] { 0.1f, 0.2f, 0.3f };
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();

        var doc1 = new Document { };
        session.Store(doc1);
        session.SaveChanges();
        using var memStream = new MemoryStream(MemoryMarshal.Cast<float, byte>(v1).ToArray());
        session.Advanced.Attachments.Store(doc1, EmbeddingName, memStream);
        session.SaveChanges();

        var index = new CSharpIndexWithVector();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);

        Document doc = session.Query<Document, CSharpIndexWithVector>()
            .VectorSearch(f => f.WithField(s => s.Vector),
                v => v.ByEmbedding(v1))
            .FirstOrDefault();

        Assert.NotNull(doc);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void CreateVectorFromMultipleAttachments()
    {
        var v1 = new[] { 0.1f, 0.2f, 0.3f };
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();

        var doc1 = new Document { };
        session.Store(doc1);
        session.SaveChanges();
        using var memStream1 = new MemoryStream(MemoryMarshal.Cast<float, byte>(v1).ToArray());
        using var memStream2 = new MemoryStream(MemoryMarshal.Cast<float, byte>(v1.Select(x => -x).ToArray()).ToArray());
        using var memStream3 = new MemoryStream(MemoryMarshal.Cast<float, byte>(v1.Select(x => x + 1.2f).ToArray()).ToArray());
        session.Advanced.Attachments.Store(doc1, EmbeddingName + 1, memStream1);
        session.Advanced.Attachments.Store(doc1, EmbeddingName + 2, memStream2);
        session.Advanced.Attachments.Store(doc1, EmbeddingName + 3, memStream3);
        session.SaveChanges();

        var index = new CSharpIndexWithMultipleVector();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);

        Document doc = session.Query<Document, CSharpIndexWithMultipleVector>()
            .VectorSearch(f => f.WithField(s => s.Vector),
                v => v.ByEmbedding(v1))
            .FirstOrDefault();

        Assert.NotNull(doc);
    }

    private class CSharpIndexWithVector : AbstractIndexCreationTask<Document>
    {
        public CSharpIndexWithVector()
        {
            Map = documents => from document in documents
                let embeddings = LoadAttachment(document, EmbeddingName)
                select new { Vector = CreateVector(embeddings.GetContentAsStream()) };
        }
    }

    private class CSharpIndexWithMultipleVector : AbstractIndexCreationTask<Document>
    {
        public CSharpIndexWithMultipleVector()
        {
            Map = documents => from document in documents
                let embeddings = LoadAttachments(document)
                select new { Vector = CreateVector(embeddings.Select(e => e.GetContentAsStream())) };
        }
    }

    private class JsIndexWithVector : AbstractJavaScriptIndexCreationTask
    {
        public JsIndexWithVector()
        {
            Maps = new HashSet<string>
            {
                $@"map('Documents', function (document) {{
                var attachment = loadAttachment(document, '{EmbeddingName}');
                return {{
                    Vector: createVector(attachment.getContentAsString('utf8'))
                }};
            }})"
            };

            Fields = new Dictionary<string, IndexFieldOptions>()
            {
                {
                    "Vector",
                    new IndexFieldOptions() { Vector = new() { SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single } }
                }
            };
        }
    }

    private class Document
    {
        public string Id { get; set; }
        public object Vector { get; set; }
    }
}
