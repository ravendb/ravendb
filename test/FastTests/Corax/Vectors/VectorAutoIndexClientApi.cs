using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class VectorAutoIndexClientApi(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void SinglesToSinglesTest(bool isExact) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(Singles)",
        fieldRqlSelector: "vector.search(Singles, $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithEmbedding(f => f.Singles), 
                value => value.ByEmbedding([0.1f, 0.1f]), isExact: isExact), isExact);
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void SinglesToInt8Test(bool isExact) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.f32_i8(Singles))",
        fieldRqlSelector: "vector.search(embedding.f32_i8(Singles), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithEmbedding(f => f.Singles).TargetQuantization(VectorEmbeddingType.Int8), 
                value => value.ByEmbedding([0.1f, 0.1f]), isExact: isExact), isExact);
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void SinglesToBinaryTest(bool isExact) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.f32_i1(Singles))",
        fieldRqlSelector: "vector.search(embedding.f32_i1(Singles), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithEmbedding(f => f.Singles).TargetQuantization(VectorEmbeddingType.Binary), 
                value => value.ByEmbedding([0.1f, 0.1f]), isExact: isExact), isExact);

    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void Int8Test(bool isExact) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.i8(Int8))",
        fieldRqlSelector: "vector.search(embedding.i8(Int8), $p0)",
        vectorWhere: docs => docs.VectorSearch(field => field.WithEmbedding(f => f.Int8, VectorEmbeddingType.Int8),
            value => value.ByEmbedding(new[]{(sbyte)-1, (sbyte)1}), isExact: isExact), isExact);
    
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void Int1Test(bool isExact) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.i1(Binary))",
        fieldRqlSelector: "vector.search(embedding.i1(Binary), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithEmbedding(f => f.Binary, VectorEmbeddingType.Binary), 
                value => value.ByEmbedding([1, 2]), isExact: isExact), isExact);
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void TextToSinglesTest(bool isExact) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.text(Text))",
        fieldRqlSelector: "vector.search(embedding.text(Text), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithText(x => x.Text), 
                value => value.ByText("test"), isExact: isExact), isExact);
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void TextToInt8Test(bool isExact) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.text_i8(Text))",
        fieldRqlSelector: "vector.search(embedding.text_i8(Text), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithText(x => x.Text).TargetQuantization(VectorEmbeddingType.Int8), 
                value => value.ByText("test"), isExact: isExact), isExact);
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void TextToInt1Test(bool isExact) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.text_i1(Text))",
        fieldRqlSelector: "vector.search(embedding.text_i1(Text), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithText(x => x.Text).TargetQuantization(VectorEmbeddingType.Binary), 
                value => value.ByText("test"), isExact: isExact), isExact);
    
    private void AutoIndexingTestingBase(string autoIndexName, string fieldRqlSelector, Func<IRavenQueryable<AutoVecDoc>, IRavenQueryable<AutoVecDoc>> vectorWhere, bool isExact)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc("Test", [1.0f, 1.0f], [-1, 1], [1, 1]));
        session.SaveChanges();
        var baseQuery = session.Query<AutoVecDoc>().Statistics(out var stats).Customize(x => x.WaitForNonStaleResults());
        baseQuery = vectorWhere(baseQuery);
        _ = baseQuery.ToList(); // evaluate

        Assert.Equal(autoIndexName, stats.IndexName);
        fieldRqlSelector = isExact ? $"exact({fieldRqlSelector})" : fieldRqlSelector;
        var rql = $"from 'AutoVecDocs' where {fieldRqlSelector}";
        Assert.Equal(rql, baseQuery.ToString());
    }

    [RavenFact(RavenTestCategory.Vector)]
    public void NonExistingFieldDoesntEndWithNre()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.SaveChanges();

        var documentQueryNonExistingField = session!.Advanced.DocumentQuery<AutoVecDoc>()
            .VectorSearch(f => f.WithText("NonExistingField"), v => v.ByText("---"))
            .Statistics(out var stats)
            .ToList();
        store.Maintenance.Send(new DeleteIndexOperation(stats.IndexName));

        var linqQueryNonExistingField = session.Query<AutoVecDoc>()
            .VectorSearch(f => f.WithText("NonExistingField"), v => v.ByText("---"))
            .Statistics(out stats)
            .ToList();
        
        WaitForUserToContinueTheTest(store);
    }
    
    private record AutoVecDoc(string Text, float[] Singles, sbyte[] Int8, byte[] Binary, string Id = null);
}
