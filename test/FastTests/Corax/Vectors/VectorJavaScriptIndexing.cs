using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Vector;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax.Vectors;

public class VectorJavaScriptIndexing(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void TextToSinglesTest(Options options) => JsIndexingTestingBase(options, nameof(VecDoc.Text), VectorEmbeddingType.Text, VectorEmbeddingType.Single, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByText("test")));

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void TextToInt8Test(Options options) => JsIndexingTestingBase(options, nameof(VecDoc.Text), VectorEmbeddingType.Text, VectorEmbeddingType.Int8, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByText("test")));

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void TextToInt1Test(Options options) => JsIndexingTestingBase(options, nameof(VecDoc.Text), VectorEmbeddingType.Text, VectorEmbeddingType.Binary, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByText("test")));


    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(nameof(VecDoc.Singles), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.SinglesBase64), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.SinglesEnumerable), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.SinglesEnumerableBase64), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void SinglesToSinglesTest(Options options, string fieldName) =>
        JsIndexingTestingBase(options, fieldName, VectorEmbeddingType.Single, VectorEmbeddingType.Single, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByEmbedding([1.0f, 1.0f])));

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(nameof(VecDoc.Singles), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.SinglesBase64), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.SinglesEnumerable), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.SinglesEnumerableBase64), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void SinglesToInt8Test(Options options, string fieldName) =>
        JsIndexingTestingBase(options, fieldName, VectorEmbeddingType.Single, VectorEmbeddingType.Int8, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByEmbedding([1.0f, 1.0f])));

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(nameof(VecDoc.Singles), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.SinglesBase64), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.SinglesEnumerable), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.SinglesEnumerableBase64), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void SinglesToInt1Test(Options options, string fieldName) =>
        JsIndexingTestingBase(options, fieldName, VectorEmbeddingType.Single, VectorEmbeddingType.Binary, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByEmbedding([1.0f, 1.0f])));

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(nameof(VecDoc.Int8), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.Int8Base64), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.Int8Enumerable), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.Int8EnumerableBase64), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void Int8Test(Options options, string fieldName) =>
        JsIndexingTestingBase(options, fieldName, VectorEmbeddingType.Int8, VectorEmbeddingType.Int8, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByEmbedding(VectorQuantizer.ToInt8([-1, 1]))));

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(nameof(VecDoc.Binary), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.BinaryBase64), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.BinaryEnumerable), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(nameof(VecDoc.BinaryEnumerableBase64), SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void Int1Test(Options options, string fieldName) => JsIndexingTestingBase(options, fieldName, VectorEmbeddingType.Binary, VectorEmbeddingType.Binary, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByEmbedding([1, 0])));

    private void JsIndexingTestingBase(Options options, string fieldName, VectorEmbeddingType src, VectorEmbeddingType dest, Func<IRavenQueryable<VecDoc>, IRavenQueryable<VecDoc>> vectorWhere)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            float[][] singles = [[1.0f, 1.0f], [-1.0f, 1.0f]];
            var single0Base64 = Convert.ToBase64String(MemoryMarshal.Cast<float, byte>(singles[0]));
            var single1Base64 = Convert.ToBase64String(MemoryMarshal.Cast<float, byte>(singles[1]));


            sbyte[][] i8 = [VectorQuantizer.ToInt8([-1, 1]), VectorQuantizer.ToInt8([-5, 5])];
            var i8_0Base64 = Convert.ToBase64String(MemoryMarshal.Cast<sbyte, byte>(i8[0]));
            var i8_1Base64 = Convert.ToBase64String(MemoryMarshal.Cast<sbyte, byte>(i8[1]));


            byte[][] i1 = [[1, 5], [25, 30]];
            var i1_0Base64 = Convert.ToBase64String(i1[0]);
            var i1_1Base64 = Convert.ToBase64String(i1[1]);

            session.Store(new VecDoc("Test", singles[0], i8[0], i1[0],
                ["Test", "tseT"], singles, i8, i1,
                single0Base64, i8_0Base64, i1_0Base64,
                [single0Base64, single1Base64], [i8_0Base64, i8_1Base64], [i1_0Base64, i1_1Base64]));
            session.SaveChanges();
        }

        new VectorIndex(fieldName, src, dest).Execute(store);
        Indexes.WaitForIndexing(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);

        using (var session = store.OpenSession())
        {
            var results = vectorWhere(session.Query<VecDoc, VectorIndex>()).ToList();
            Assert.Equal(1, results.Count);


            if (options.DatabaseMode is RavenDatabaseMode.Sharded)
            {
                var metadata = session.Advanced.GetMetadataFor(results[0]);
                Assert.True(metadata.ContainsKey("@index-score"));
                var score = Convert.ToDouble(metadata["@index-score"]);
                Assert.NotEqual(0, score);
            }
        }
    }

    private class VectorIndex : AbstractJavaScriptIndexCreationTask
    {
        public VectorIndex()
        {
            //querying    
        }

        public VectorIndex(string fieldName, VectorEmbeddingType source, VectorEmbeddingType destination)
        {
            Maps =
            [
                @$"map('VecDocs', function (e) {{
    return {{ 
        Name: e.Name,
        Vector: createVector(e.{fieldName})
    }};
}})"
            ];

            Fields = new Dictionary<string, IndexFieldOptions>()
            {
                {
                    "Vector",
                    new IndexFieldOptions { Vector = new VectorOptions() { SourceEmbeddingType = source, DestinationEmbeddingType = destination } }
                }
            };
        }
    }

    private record VecDoc(
        string Text,
        float[] Singles,
        sbyte[] Int8,
        byte[] Binary,
        string[] TextEnumerable,
        float[][] SinglesEnumerable,
        sbyte[][] Int8Enumerable,
        byte[][] BinaryEnumerable,
        string SinglesBase64,
        string Int8Base64,
        string BinaryBase64,
        string[] SinglesEnumerableBase64,
        string[] Int8EnumerableBase64,
        string[] BinaryEnumerableBase64,
        string Id = null,
        object Vector = null);
}
