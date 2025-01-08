using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;
using FastTests;
using FastTests.Voron.FixedSize;
using Raven.Client.Documents;
using Raven.Server.Config;
using Sparrow;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Voron.Data.Graphs;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_23442(ITestOutputHelper output) : RavenTestBase(output)
{
    private static IEnumerable<object[]> Theory()
    {
        foreach (var sortVectorSearchByScoreAutomatically in new[] { false, true })
        {
            foreach (var includeScores in new[] { false, true })
            {
                yield return [sortVectorSearchByScoreAutomatically, includeScores];
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [MemberData(nameof(Theory))]
    public void SingleVectorSearchWillAlwaysBeSortedByDistance(bool sortVectorSearchByScoreAutomatically, bool includeScore)
    {
        using var store = GetDocumentStoreWithDocuments(sortVectorSearchByScoreAutomatically, includeScore);
        using var session = store.OpenSession();

        var results = session.Query<Product>()
            .Customize(c => c.WaitForNonStaleResults())
            .VectorSearch(f => f.WithText(p => p.Name),
                v => v.ByText("vehicle"), minimumSimilarity: 0.1f)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("car", results[0].Name);
        Assert.Equal("banana", results[1].Name);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [MemberData(nameof(Theory))]
    public void VectorSearchWillAlwaysBeSortedByDistance(bool sortVectorSearchByScoreAutomatically, bool includeScore)
    {
        using var store = GetDocumentStoreWithDocuments(sortVectorSearchByScoreAutomatically, includeScore);
        using var session = store.OpenSession();

        var results = session.Advanced.DocumentQuery<Product>()
            .WaitForNonStaleResults()
            .WhereEquals(x => x.Discontinued, true)
            .AndAlso()
            .VectorSearch(f => f.WithText(p => p.Name),
                v => v.ByText("vehicle"), minimumSimilarity: 0.1f)
            .ToList();

        Assert.Equal(2, results.Count);
        if (sortVectorSearchByScoreAutomatically)
        {
            Assert.Equal("car", results[0].Name);
            Assert.Equal("banana", results[1].Name);
        }
        else
        {
            Assert.Equal("banana", results[0].Name);
            Assert.Equal("car", results[1].Name);
        }
    }

    private IDocumentStore GetDocumentStoreWithDocuments(bool sortVectorSearchByScoreAutomatically, bool coraxIncludeDocumentScore)
    {
        var sourceOptions = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        sourceOptions.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxVectorSearchOrderByScoreAutomatically)] = sortVectorSearchByScoreAutomatically.ToString();
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = coraxIncludeDocumentScore.ToString();
        };

        var store = GetDocumentStore(sourceOptions);
        using var session = store.OpenSession();
        session.Store(new Product() { Name = "banana", Discontinued = true });
        session.Store(new Product() { Name = "car", Discontinued = true });
        session.SaveChanges();
        return store;
    }

    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void SimdCosineSimilarityToScore(int seed)
    {
        var size = GetSizeForSimdTest();
        var random = new System.Random(seed);
        var originalArray = Enumerable.Range(0, size).Select(x => random.NextSingle()).ToArray();
        var clonedArray = originalArray.ToArray();

        Hnsw.DistanceToScoreCosineSimilarity(clonedArray);
        for (int i = 0; i < originalArray.Length; i++)
            Assert.Equal(1f - originalArray[i], clonedArray[i], float.Epsilon);
    }

    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void SimdHammingSimilarityToScore(int seed)
    {
        var size = GetSizeForSimdTest();
        var random = new System.Random(seed);
        var originalArray = Enumerable.Range(0, size).Select(x => (float)random.Next(0, 64)).ToArray();
        var clonedArray = originalArray.ToArray();

        Hnsw.DistanceToScoreHammingSimilarity(clonedArray, 8);
        for (int i = 0; i < originalArray.Length; i++)
            Assert.Equal(1f - (originalArray[i] / 64f), clonedArray[i], float.Epsilon);
    }

    private static int GetSizeForSimdTest()
    {
        var size = 1;
        if (AdvInstructionSet.IsAcceleratedVector512)
            size += Vector512<float>.Count;
        if (AdvInstructionSet.IsAcceleratedVector256)
            size += Vector256<float>.Count;
        if (AdvInstructionSet.IsAcceleratedVector128)
            size += Vector128<float>.Count;
        return size;
    }
}
