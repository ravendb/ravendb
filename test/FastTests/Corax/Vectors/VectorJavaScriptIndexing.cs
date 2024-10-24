using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Linq;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class VectorJavaScriptIndexing : RavenTestBase
{
    public VectorJavaScriptIndexing(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void TextToSinglesTest() => JsIndexingTestingBase(nameof(VecDoc.Text), EmbeddingType.Text, EmbeddingType.Single, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByText("test")));
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void TextToInt8Test() => JsIndexingTestingBase(nameof(VecDoc.Text), EmbeddingType.Text, EmbeddingType.Int8, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByText("test")));
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void TextToInt1Test() => JsIndexingTestingBase(nameof(VecDoc.Text), EmbeddingType.Text, EmbeddingType.Binary, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByText("test")));
    
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(nameof(VecDoc.Singles))]
    [InlineData(nameof(VecDoc.SinglesBase64))]
    [InlineData(nameof(VecDoc.SinglesEnumerable))]
    [InlineData(nameof(VecDoc.SinglesEnumerableBase64))]
    public void SinglesToSinglesTest(string fieldName) => JsIndexingTestingBase(fieldName, EmbeddingType.Single, EmbeddingType.Single, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByEmbedding([1.0f, 1.0f])));
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(nameof(VecDoc.Singles))]
    [InlineData(nameof(VecDoc.SinglesBase64))]
    [InlineData(nameof(VecDoc.SinglesEnumerable))]
    [InlineData(nameof(VecDoc.SinglesEnumerableBase64))]
    public void SinglesToInt8Test(string fieldName) => JsIndexingTestingBase(fieldName, EmbeddingType.Single, EmbeddingType.Int8, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByEmbedding([1.0f, 1.0f])));
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(nameof(VecDoc.Singles))]
    [InlineData(nameof(VecDoc.SinglesBase64))]
    [InlineData(nameof(VecDoc.SinglesEnumerable))]
    [InlineData(nameof(VecDoc.SinglesEnumerableBase64))]
    public void SinglesToInt1Test(string fieldName) => JsIndexingTestingBase(fieldName, EmbeddingType.Single, EmbeddingType.Binary, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByEmbedding([1.0f, 1.0f])));
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(nameof(VecDoc.Int8))]
    [InlineData(nameof(VecDoc.Int8Base64))]
    [InlineData(nameof(VecDoc.Int8Enumerable))]
    [InlineData(nameof(VecDoc.Int8EnumerableBase64))]
    public void Int8Test(string fieldName) => JsIndexingTestingBase(fieldName, EmbeddingType.Int8, EmbeddingType.Int8, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByEmbedding([-1, 1])));
        
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(nameof(VecDoc.Binary))]
    [InlineData(nameof(VecDoc.BinaryBase64))]
    [InlineData(nameof(VecDoc.BinaryEnumerable))]
    [InlineData(nameof(VecDoc.BinaryEnumerableBase64))]
    public void Int1Test(string fieldName) => JsIndexingTestingBase(fieldName, EmbeddingType.Binary, EmbeddingType.Binary, docs => docs.VectorSearch(f => f.WithField(x => x.Vector), v => v.ByEmbedding([1, 0])));
    
    private void JsIndexingTestingBase(string fieldName, EmbeddingType src, EmbeddingType dest, Func<IRavenQueryable<VecDoc>, IRavenQueryable<VecDoc>> vectorWhere)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();

        float[][] singles = [[1.0f, 1.0f], [-1.0f, 1.0f]];
        var single0Base64 = Convert.ToBase64String(MemoryMarshal.Cast<float, byte>(singles[0]));
        var single1Base64 = Convert.ToBase64String(MemoryMarshal.Cast<float, byte>(singles[1]));
        
        
        sbyte[][] i8 = [[-1, 1], [-5, 5]];
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

        new VectorIndex(fieldName, src, dest).Execute(store);
        Indexes.WaitForIndexing(store);
        
        //WaitForUserToContinueTheTest(store);
        var baseQuery = session.Query<VecDoc, VectorIndex>().Statistics(out var stats).Customize(x => x.WaitForNonStaleResults());
        baseQuery = vectorWhere(baseQuery);
        var result = baseQuery.Single(); // evaluate and assert
    }
    
    
    private class VectorIndex : AbstractJavaScriptIndexCreationTask
    {
        public VectorIndex()
        {
            //querying    
        }
        
        public VectorIndex(string fieldName, EmbeddingType source, EmbeddingType destination)
        {
            Maps = [@$"map('VecDocs', function (e) {{
    return {{ 
        Name: e.Name,
        Vector: createVectorSearch(e.{fieldName})
    }};
}})"];

            Fields = new Dictionary<string, IndexFieldOptions>()
            {
                {
                    "Vector",
                    new IndexFieldOptions { Vector = new VectorOptions() { SourceEmbeddingType = source, DestinationEmbeddingType = destination } }
                }
            };
        }
    }
    
    private record VecDoc(string Text, float[] Singles, sbyte[] Int8, byte[] Binary, 
        string[] TextEnumerable, float[][] SinglesEnumerable, sbyte[][] Int8Enumerable, byte[][] BinaryEnumerable, 
        string SinglesBase64, string Int8Base64, string BinaryBase64,
        string[] SinglesEnumerableBase64, string[] Int8EnumerableBase64, string[] BinaryEnumerableBase64, 

        string Id = null, object Vector = null);
}
