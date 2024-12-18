using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Vector;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class VectorAutoIndexClientApi(ITestOutputHelper output) : RavenTestBase(output)
{
    public static IEnumerable<object[]> GenerateScenarios()
    {
        bool[] exactConfig = [ false];
        bool[] isNativeVector = [ false];
        Func<object, Action<IVectorEmbeddingFieldValueFactory>> primitiveValueFunc = (data =>
        {
            return data switch
            {
                float[] fArray => factory => factory.ByEmbedding(fArray),
                sbyte[] i8Array => factory => factory.ByEmbedding(i8Array),
                byte[] i1Array => factory => factory.ByEmbedding(i1Array),
                _ => throw new ArgumentException("Invalid data type")
            };
        });


        Func<object, Action<IVectorEmbeddingFieldValueFactory>> ravenVectorValueFunc = (data =>
        {
            return data switch
            {
                float[] fArray => factory => factory.ByEmbedding(new RavenVector<float>(fArray)),
                sbyte[] i8Array => factory => factory.ByEmbedding(new RavenVector<sbyte>(i8Array)),
                byte[] i1Array => factory => factory.ByEmbedding(new RavenVector<byte>(i1Array)),
                _ => throw new ArgumentException("Invalid data type")
            };
        });

        foreach (var isExact in exactConfig)
        {
            foreach (var isNative in isNativeVector)
            {
                yield return [isExact, isNative, isNative ? primitiveValueFunc : ravenVectorValueFunc];
            }
        }
    }

    [RavenTheory(RavenTestCategory.Vector)]
    [MemberData(nameof(GenerateScenarios))]
    public void SinglesToSinglesTest(bool isExact, bool isNative, Func<object, Action<IVectorEmbeddingFieldValueFactory>> embedding) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(Singles)",
        fieldRqlSelector: "vector.search(Singles, $p0)",
        vectorWhere: docs => docs.VectorSearch(field => field.WithEmbedding(f => f.Singles),
            embedding(new float[] {0.1f, 0.1f}), isExact: isExact), isExact);

    [RavenTheory(RavenTestCategory.Vector)]
    [MemberData(nameof(GenerateScenarios))]
    public void SinglesToInt8Test(bool isExact, bool isNative, Func<object, Action<IVectorEmbeddingFieldValueFactory>> embedding) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.f32_i8(Singles))",
        fieldRqlSelector: "vector.search(embedding.f32_i8(Singles), $p0)",
        vectorWhere: docs => docs.VectorSearch(field => field.WithEmbedding(f => f.Singles).TargetQuantization(VectorEmbeddingType.Int8), embedding(new float[] { 0.1f, 0.1f }), isExact: isExact), isExact);

    [RavenTheory(RavenTestCategory.Vector)]
    [MemberData(nameof(GenerateScenarios))]
    public void SinglesToBinaryTest(bool isExact, bool isNative, Func<object, Action<IVectorEmbeddingFieldValueFactory>> embedding) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.f32_i1(Singles))",
        fieldRqlSelector: "vector.search(embedding.f32_i1(Singles), $p0)",
        vectorWhere: docs => docs.VectorSearch(field => field.WithEmbedding(f => f.Singles).TargetQuantization(VectorEmbeddingType.Binary), embedding(new float[]{ 0.1f, 0.1f }), isExact: isExact), isExact);

    [RavenTheory(RavenTestCategory.Vector)]
    [MemberData(nameof(GenerateScenarios))]
    public void Int8Test(bool isExact, bool isNative, Func<object, Action<IVectorEmbeddingFieldValueFactory>> embedding) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.i8(Int8))",
        fieldRqlSelector: "vector.search(embedding.i8(Int8), $p0)",
        vectorWhere: docs => docs.VectorSearch(field => field.WithEmbedding(f => f.Int8, VectorEmbeddingType.Int8),
            embedding(VectorQuantizer.ToInt8([-1f, 1f])), isExact: isExact), isExact);


    [RavenTheory(RavenTestCategory.Vector)]
    [MemberData(nameof(GenerateScenarios))]
    public void Int1Test(bool isExact, bool isNative, Func<object, Action<IVectorEmbeddingFieldValueFactory>> embedding) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.i1(Binary))",
        fieldRqlSelector: "vector.search(embedding.i1(Binary), $p0)",
        vectorWhere: docs => docs.VectorSearch(field => field.WithEmbedding(f => f.Binary, VectorEmbeddingType.Binary),
            embedding(VectorQuantizer.ToInt1([1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0])), isExact: isExact), isExact);

    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void TextToSinglesTest(bool isExact) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.text(Text))",
        fieldRqlSelector: "vector.search(embedding.text(Text), $p0)",
        vectorWhere: docs => docs.VectorSearch(field => field.WithText(x => x.Text),
            value => value.ByText("test"), isExact: isExact), isExact);

    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void TextToInt8Test(bool isExact) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.text_i8(Text))",
        fieldRqlSelector: "vector.search(embedding.text_i8(Text), $p0)",
        vectorWhere: docs => docs.VectorSearch(field => field.WithText(x => x.Text).TargetQuantization(VectorEmbeddingType.Int8),
            value => value.ByText("test"), isExact: isExact), isExact);

    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void TextToInt1Test(bool isExact) => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.text_i1(Text))",
        fieldRqlSelector: "vector.search(embedding.text_i1(Text), $p0)",
        vectorWhere: docs => docs.VectorSearch(field => field.WithText(x => x.Text).TargetQuantization(VectorEmbeddingType.Binary),
            value => value.ByText("test"), isExact: isExact), isExact);

    private void AutoIndexingTestingBase(string autoIndexName, string fieldRqlSelector, Func<IRavenQueryable<AutoVecDoc>, IRavenQueryable<AutoVecDoc>> vectorWhere,
        bool isExact)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc("Test", [1.0f, 1.0f], VectorQuantizer.ToInt8([-1, 1]), VectorQuantizer.ToInt1([1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0])));
        session.SaveChanges();
        var baseQuery = session.Query<AutoVecDoc>().Statistics(out var stats).Customize(x => x.WaitForNonStaleResults());
        baseQuery = vectorWhere(baseQuery);
        _ = baseQuery.ToList(); // evaluate
        WaitForUserToContinueTheTest(store);

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
