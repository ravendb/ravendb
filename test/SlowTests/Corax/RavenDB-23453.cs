using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Org.BouncyCastle.Security;
using Raven.Server.Documents.Indexes.VectorSearch;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;
using VectorOptions = Raven.Client.Documents.Indexes.Vector.VectorOptions;

namespace SlowTests.Corax;

public class RavenDB_23453(ITestOutputHelper output) : StorageTest(output)
{
    private const float MinSimilarity = 0.75f;
    private const int TopKMulti = 32;
    private const long FilterMin = 15L;
    private const long FilterMax = 18L;


    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineData(true, false, 1479157058)] //HNSW level-0 had unreachable nodes for this seed — see resultQueryContainsAllDb
    [InlineDataWithRandomSeed(true, false)]
    [InlineDataWithRandomSeed(false, true)]
    [InlineDataWithRandomSeed(true, true)]
    public void CanFilterVectors(bool shouldScan, bool isExact, int seed)
    {
        var (resultQueryContainsAllDb, resultsFromFilteredQuery, resultsWithoutFilterQuery, manualQuery, docs) = CanFilterVectorsBase(shouldScan, isExact, seed);
        Assert.Equal(16, manualQuery.Count);
        Assert.Equal(16, resultQueryContainsAllDb.Count);
        Assert.Equal(16, resultsFromFilteredQuery.Count);

        foreach (var doc in resultsFromFilteredQuery)
        {
            var docInstance = docs.Single(x => x.Id == doc);
            Assert.True(docInstance.Numerical is >= 15 and <= 18);
        }

        Assert.Equal(16, resultsWithoutFilterQuery.Count);
        Assert.NotEqual(0, resultsWithoutFilterQuery.Count(x => resultsFromFilteredQuery.Contains(x) == false));
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineData(1980969695)]       
    [InlineData(720473198)]       

    public void CanFilterVectorsNearestSearch(int seed)
    {
        // NearestSearch must have a more relaxed assertion since the graph structure depends on the seed. We may get all but we can get partial results, However we should (since it's not a big graph), get at least one result.  
        var (_, resultsFromFilteredQuery, _, _, docs) = CanFilterVectorsBase(false, false, seed);

        Assert.NotEmpty(resultsFromFilteredQuery);
        foreach (var doc in resultsFromFilteredQuery)
        {
            var docInstance = docs.Single(x => x.Id == doc);
            Assert.True(docInstance.Numerical is >= 15 and <= 18);
        }
    }

    /// <summary>
    /// ResultQueryContainsAllDb – VectorSearch scans all vectors in the index; AND operates on all vectors returned by HNSW.
    /// ResultsFromFilteredQuery – VectorSearch uses a vector filter, limiting the number of vectors scanned (depending on query configuration).
    /// ResultsWithoutFilterQuery – VectorSearch has no vector filter or additional filter and returns the best NoC vectors.
    /// ManualQuery – In-memory vector search via LINQ in the test.
    /// Docs – All indexed documents.
    /// </summary>
    private (List<string> ResultQueryContainsAllDb, List<string> ResultsFromFilteredQuery, List<string> ResultsWithoutFilterQuery, List<VectorSearchResult> ManualQuery, List<Doc> Docs) CanFilterVectorsBase(bool shouldScan, bool isExact, int seed)
    {
        using var mapping = GetMapping();
        var docs = GetDocs(360).ToList();
        Random random = new(seed);
        IndexDocuments(Env, mapping, docs, random);

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            List<string> resultQueryContainsAllDb;
            List<string> resultsFromFilteredQuery;
            List<string> resultsWithoutFilterQuery;
            var manualQuery = VectorSearchInPlace(docs, p => p.Numerical >= 15 && p.Numerical <= 18, [docs[0].Vector], MinSimilarity, TopKMulti).ToList();

            {
                var betweenQuery = indexSearcher.BetweenQuery(mapping.GetByFieldId(2).Metadata.ChangeScoringMode(true), 15L, 18L);
                var vec1 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[0].Vector), VectorOptions.Default);
                var vecSearch = indexSearcher.VectorSearch(mapping.GetByFieldId(3).Metadata.ChangeScoringMode(true), vec1, 0.75f, 400, isExact: true, false, random: random);
                IQueryMatch query = indexSearcher.And(vecSearch, betweenQuery);
                query = indexSearcher.OrderBy(query, [new OrderMetadata(true, MatchCompareFieldType.Score)], defaultNullsSortMode: NullsSortMode.NullsSmallest);
                resultQueryContainsAllDb = EvaluateQuery(indexSearcher, ref query);
            }
            {
                var betweenQuery = indexSearcher.BetweenQuery(mapping.GetByFieldId(2).Metadata.ChangeScoringMode(true), FilterMin, FilterMax);
                var vec1 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[0].Vector), VectorOptions.Default);
                var vecSearch = indexSearcher.VectorSearch(mapping.GetByFieldId(3).Metadata.ChangeScoringMode(true), vec1, 0.75f, 16, isExact, false, betweenQuery, shouldScan ? 1024 : 0, random: random);
                var query = indexSearcher.OrderBy(vecSearch, [new OrderMetadata(true, MatchCompareFieldType.Score)], defaultNullsSortMode: NullsSortMode.NullsSmallest);
                resultsFromFilteredQuery = EvaluateQuery(indexSearcher, ref query);
            }

            {
                var vec1 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[0].Vector), VectorOptions.Default);
                var vecSearch = indexSearcher.VectorSearch(mapping.GetByFieldId(3).Metadata.ChangeScoringMode(true), vec1, 0.75f, 16, isExact, false, random: random);
                var query = indexSearcher.OrderBy(vecSearch, [new OrderMetadata(true, MatchCompareFieldType.Score)], defaultNullsSortMode: NullsSortMode.NullsSmallest);
                resultsWithoutFilterQuery = EvaluateQuery(indexSearcher, ref query);
            }

            return (resultQueryContainsAllDb, resultsFromFilteredQuery, resultsWithoutFilterQuery, manualQuery, docs);
        }
    }
    
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(true, false)]
    [InlineDataWithRandomSeed(false, true)]
    [InlineDataWithRandomSeed(true, true)]
    public void CanFilterMultiVectors(bool shouldScan, bool isExact, int seed)
    {
        var (resultQueryContainsAllDb, resultsFromFilteredQuery, resultsWithoutFilterQuery, manualQuery, docs) = CanFilterMultiVectorsBase(shouldScan, isExact, seed);
        Assert.Equal(32, manualQuery.Count);
        Assert.Equal(32, resultQueryContainsAllDb.Count);
        Assert.Equal(32, resultsFromFilteredQuery.Count);

        foreach (var doc in resultsFromFilteredQuery)
        {
            var docInstance = docs.Single(x => x.Id == doc);
            Assert.True(docInstance.Numerical is >= 15 and <= 18);
        }

        Assert.Equal(32, resultsWithoutFilterQuery.Count);
        Assert.NotEqual(0, resultsWithoutFilterQuery.Count(x => resultsFromFilteredQuery.Contains(x) == false));
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void CanFilterMultiVectorsNearest(int seed)
    {
        var (_, resultsFromFilteredQuery, _, _, docs) = CanFilterMultiVectorsBase(false, false, seed);

        Assert.NotEmpty(resultsFromFilteredQuery);
        foreach (var doc in resultsFromFilteredQuery)
        {
            var docInstance = docs.Single(x => x.Id == doc);
            Assert.True(docInstance.Numerical is >= 15 and <= 18);
        }

    }
    
    /// <summary>
    /// ResultQueryContainsAllDb – VectorSearch scans all vectors in the index; AND operates on all vectors returned by HNSW.
    /// ResultsFromFilteredQuery – VectorSearch uses a vector filter, limiting the number of vectors scanned (depending on query configuration).
    /// ResultsWithoutFilterQuery – VectorSearch has no vector filter or additional filter and returns the best NoC vectors.
    /// ManualQuery – In-memory vector search via LINQ in the test.
    /// Docs – All indexed documents.
    /// </summary>
    private (List<string> ResultQueryContainsAllDb, List<string> ResultsFromFilteredQuery, List<string> ResultsWithoutFilterQuery, List<VectorSearchResult> ManualQuery, List<Doc> Docs) CanFilterMultiVectorsBase(bool shouldScan, bool isExact, int seed)
    {
        using var mapping = GetMapping();
        var docs = GetDocs(360).ToList();
        var random = new Random(seed);
        IndexDocuments(Env, mapping, docs, random);

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            List<string> resultQueryContainsAllDb;
            List<string> resultsFromFilteredQuery;
            List<string> resultsWithoutFilterQuery;
            var manualQuery = VectorSearchInPlace(docs, p => p.Numerical >= 15 && p.Numerical <= 18, [docs[0].Vector, docs[180].Vector], MinSimilarity, TopKMulti).ToList();

            {
                var betweenQuery = indexSearcher.BetweenQuery(mapping.GetByFieldId(2).Metadata.ChangeScoringMode(true), FilterMin, FilterMax);
                var vec1 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[0].Vector), VectorOptions.Default);
                var vec2 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[180].Vector), VectorOptions.Default);
                var vecSearch = indexSearcher.MultiVectorSearch(mapping.GetByFieldId(3).Metadata.ChangeScoringMode(true), [vec1, vec2], 0.75f, 400, isExact: true, false, random: random);
                IQueryMatch query = indexSearcher.And(vecSearch, betweenQuery);
                query = indexSearcher.OrderBy(query, [new OrderMetadata(true, MatchCompareFieldType.Score)], defaultNullsSortMode: NullsSortMode.NullsSmallest);
                resultQueryContainsAllDb = EvaluateQuery(indexSearcher, ref query);
            }
            {
                var betweenQuery = indexSearcher.BetweenQuery(mapping.GetByFieldId(2).Metadata.ChangeScoringMode(true), FilterMin, FilterMax);
                var vec1 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[0].Vector), VectorOptions.Default);
                var vec2 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[180].Vector), VectorOptions.Default);
                var vecSearch = indexSearcher.MultiVectorSearch(mapping.GetByFieldId(3).Metadata.ChangeScoringMode(true), [vec1, vec2], 0.75f, 16, isExact, false, betweenQuery, shouldScan ? 1024 : 0, random: random);
                var query = indexSearcher.OrderBy(vecSearch, [new OrderMetadata(true, MatchCompareFieldType.Score)], defaultNullsSortMode: NullsSortMode.NullsSmallest);
                resultsFromFilteredQuery = EvaluateQuery(indexSearcher, ref query);
            }

            {
                var vec1 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[0].Vector), VectorOptions.Default);
                var vec2 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[180].Vector), VectorOptions.Default);
                var vecSearch = indexSearcher.MultiVectorSearch(mapping.GetByFieldId(3).Metadata.ChangeScoringMode(true), [vec1, vec2], 0.75f, 16, isExact, false, random: random);
                var query = indexSearcher.OrderBy(vecSearch, [new OrderMetadata(true, MatchCompareFieldType.Score)], defaultNullsSortMode: NullsSortMode.NullsSmallest);
                resultsWithoutFilterQuery = EvaluateQuery(indexSearcher, ref query);
            }

            return (resultQueryContainsAllDb, resultsFromFilteredQuery, resultsWithoutFilterQuery, manualQuery, docs);
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(true, false)]
    [InlineDataWithRandomSeed(false, true)]
    [InlineDataWithRandomSeed(true, true)]
    [InlineDataWithRandomSeed(false, false, Skip = "Too small set")]
    public void FilterQueryAndWithMethodVectorSearch(bool shouldScan, bool isExact, int seed)
    {
        using var mapping = GetMapping();
        var docs = GetDocs(360).ToList();
        var random = new Random(seed);
        IndexDocuments(Env, mapping, docs, random);

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            List<string> resultsFromFilteredQuery;
            List<string> andWithResults;
            var manualQuery = VectorSearchInPlace(docs, p => p.Numerical >= 15 && p.Numerical <= 18, [docs[0].Vector], MinSimilarity, TopKMulti).ToList();

            {
                var betweenQuery = indexSearcher.BetweenQuery(mapping.GetByFieldId(2).Metadata.ChangeScoringMode(true), FilterMin, FilterMax);
                var vec1 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[0].Vector), VectorOptions.Default);
                var vecSearch = indexSearcher.VectorSearch(mapping.GetByFieldId(3).Metadata.ChangeScoringMode(true), vec1, 0.75f, 400, isExact, false, betweenQuery, shouldScan ? 1024 : 0, random: random);
                var query = indexSearcher.OrderBy(vecSearch, [new OrderMetadata(true, MatchCompareFieldType.Score)], defaultNullsSortMode: NullsSortMode.NullsSmallest);
                resultsFromFilteredQuery = EvaluateQuery(indexSearcher, ref query);
            }

            {
                var betweenQuery = indexSearcher.BetweenQuery(mapping.GetByFieldId(2).Metadata.ChangeScoringMode(true), FilterMin, FilterMax);
                var vec1 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[0].Vector), VectorOptions.Default);
                var vecSearch = indexSearcher.VectorSearch(mapping.GetByFieldId(3).Metadata.ChangeScoringMode(true), vec1, 0.75f, 400, isExact, false, betweenQuery, shouldScan ? 1024 : 0, random);
                var allEntries = indexSearcher.AllEntries();
                List<long> matched = new();
                Span<long> ids = new long[16];
                while (allEntries.Fill(ids) is var read and > 0)
                {
                    var common = vecSearch.AndWith(ids, read);
                    matched.AddRange(ids[..common]);
                }

                andWithResults = GetDocsIds(indexSearcher, CollectionsMarshal.AsSpan(matched));
            }

            Assert.Equal(16, manualQuery.Count);
            Assert.Equal(16, resultsFromFilteredQuery.Count);

            foreach (var doc in resultsFromFilteredQuery)
            {
                var docInstance = docs.Single(x => x.Id == doc);
                Assert.True(docInstance.Numerical is >= 15 and <= 18);
            }

            resultsFromFilteredQuery.Sort();
            andWithResults.Sort();
            Assert.Equal(16, andWithResults.Count);
            Assert.Equal(resultsFromFilteredQuery, andWithResults);
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(true, false)]
    [InlineDataWithRandomSeed(false, true)]
    [InlineDataWithRandomSeed(true, true)]
    [InlineData(true, true, 1045861081)]
    [InlineDataWithRandomSeed(false, false, Skip = "Too small set")]
    public void FilterQueryAndWithMethodMultiVectorSearch(bool shouldScan, bool isExact, int seed)
    {
        using var mapping = GetMapping();
        var docs = GetDocs(360).ToList();

        Random random = new (seed);
        IndexDocuments(Env, mapping, docs, random);

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            List<string> resultsFromFilteredQuery;
            List<string> andWithResults;

            {
                var betweenQuery = indexSearcher.BetweenQuery(mapping.GetByFieldId(2).Metadata.ChangeScoringMode(true), FilterMin, FilterMax);
                var vec1 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[0].Vector), VectorOptions.Default);
                var vec2 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[180].Vector), VectorOptions.Default);
                var vecSearch = indexSearcher.MultiVectorSearch(mapping.GetByFieldId(3).Metadata.ChangeScoringMode(true), [vec1, vec2], 0.75f, 16, isExact, false, betweenQuery, shouldScan ? 1024 : 0, random: random);
                var query = indexSearcher.OrderBy(vecSearch, [new OrderMetadata(true, MatchCompareFieldType.Score)], defaultNullsSortMode: NullsSortMode.NullsSmallest);
                resultsFromFilteredQuery = EvaluateQuery(indexSearcher, ref query);
            }

            {
                var betweenQuery = indexSearcher.BetweenQuery(mapping.GetByFieldId(2).Metadata.ChangeScoringMode(true), FilterMin, FilterMax);
                var vec1 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[0].Vector), VectorOptions.Default);
                var vec2 = GenerateEmbeddings.FromArray(indexSearcher.Transaction.Allocator, MemoryMarshal.Cast<float, byte>(docs[180].Vector), VectorOptions.Default);
                var vecSearch = indexSearcher.MultiVectorSearch(mapping.GetByFieldId(3).Metadata.ChangeScoringMode(true), [vec1, vec2], 0.75f, 16, isExact, false, betweenQuery, shouldScan ? 1024 : 0, random: random);
                var allEntries = indexSearcher.AllEntries();
                List<long> matched = new();
                Span<long> ids = new long[16];
                while (allEntries.Fill(ids) is var read and > 0)
                {
                    var common = vecSearch.AndWith(ids, read);
                    matched.AddRange(ids[..common]);
                }

                andWithResults = GetDocsIds(indexSearcher, CollectionsMarshal.AsSpan(matched));
            }

            Assert.Equal(32, resultsFromFilteredQuery.Count);

            foreach (var doc in resultsFromFilteredQuery)
            {
                var docInstance = docs.Single(x => x.Id == doc);
                Assert.True(docInstance.Numerical is >= 15 and <= 18);
            }

            resultsFromFilteredQuery.Sort();
            andWithResults.Sort();
            Assert.Equal(32, andWithResults.Count);
            Assert.Equal(resultsFromFilteredQuery, andWithResults);
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void ExposedNearestSearchNumberOfCandidatesIsRespected(int seed)
    {
        using var mapping = GetMapping();
        
        var docs = GetDocs(360).ToList();

        Random random = new(seed);
        IndexDocuments(Env, mapping, docs, random);

        using (var rTx = Env.ReadTransaction())
        {
            using var vec1 = GenerateEmbeddings.FromArray(rTx.Allocator, MemoryMarshal.Cast<float, byte>(docs[0].Vector), VectorOptions.Default);
            using var search = Hnsw.ApproximateNearest(rTx.LowLevelTransaction, mapping.GetByFieldId(3).FieldName, 16, vec1.GetEmbeddingMemory(), 0, true);
            
            Assert.Equal(16, search.NumberOfCandidates); 
        }
    }


    private List<string> EvaluateQuery<TQueryMatch>(IndexSearcher indexSearcher, ref TQueryMatch queryMatch)
        where TQueryMatch : IQueryMatch
    {
        var results = new List<long>();
        Span<long> ids = new long[16];
        while (queryMatch.Fill(ids) is var read and > 0)
        {
            results.AddRange(ids[..read]);
        }

        return GetDocsIds(indexSearcher, CollectionsMarshal.AsSpan(results));
    }

    private List<string> GetDocsIds(IndexSearcher indexSearcher, Span<long> ids, string field = "id()")
    {
        using var termReader = indexSearcher.TermsReaderFor("id()");
        var resultsIds = new List<string>();
        foreach (var id in ids)
        {
            Assert.True(termReader.TryGetTermFor(id, out var idAsStr));
            resultsIds.Add(idAsStr);
        }

        return resultsIds;
    }

    private IndexFieldsMapping GetMapping() => IndexFieldsMappingBuilder.CreateForWriter(false)
        .AddBinding(0, "id()")
        .AddBinding(1, "Text")
        .AddBinding(2, "Numerical")
        .AddBinding(3, "Vector", vectorOptions: new global::Corax.Mappings.VectorOptions()
        {
            NumberOfCandidates = 16,
            NumberOfEdges = 12,
            VectorEmbeddingType = VectorEmbeddingType.Single
        })
        .Build();

    private static IEnumerable<Doc> GetDocs(int amount)
    {
        for (int i = 0; i < amount; ++i)
        {
            var x = MathF.Cos(i * 2 * MathF.PI / amount);
            var y = MathF.Sin(i * 2 * MathF.PI / amount);
            yield return new Doc($"doc/{i}", $"Text{i}", i % 30, [x, y]);
        }
    }

    private IEnumerable<VectorSearchResult> VectorSearchInPlace(List<Doc> docs, Predicate<Doc> booleanClause, IEnumerable<float[]> vectors, float minimumSimilarity, int numberOfCandidates)
    {
        var allVectorSearch = from doc in docs
            from vector in vectors
            let distance = 1 - System.Numerics.Tensors.TensorPrimitives.CosineSimilarity(doc.Vector, vector)
            where booleanClause(doc) && distance <= 2f * (1.0f - minimumSimilarity) + 0.001f //eps
            orderby distance
            select new VectorSearchResult(doc.Id, distance, doc.Vector);

        var vectorSearch = from vector in allVectorSearch
            group vector by vector.Id
            into g
            select new VectorSearchResult(g.Key, g.Min(x => x.Distance), g.First().Vector);

        HashSet<string> nodesReturned = new();

        foreach (var doc in vectorSearch)
        {
            var baseOfVector = Convert.ToBase64String(MemoryMarshal.Cast<float, byte>(doc.Vector));
            nodesReturned.Add(baseOfVector);
            if (nodesReturned.Count > numberOfCandidates)
                yield break;
            yield return doc;
        }
    }

    private static void IndexDocuments(StorageEnvironment env, IndexFieldsMapping mapping, IEnumerable<Doc> docs, Random random)
    {
        using var indexWriter = new IndexWriter(env, mapping, SupportedFeatures.All);
        foreach (var doc in docs)
        {
            using var builder = indexWriter.Index(doc.Id);
            builder.Write(0, Encodings.Utf8.GetBytes(doc.Id));
            builder.Write(1, Encodings.Utf8.GetBytes(doc.Text));
            builder.Write(2, Encodings.Utf8.GetBytes(doc.Numerical.ToString()), doc.Numerical, doc.Numerical);
            builder.WriteVector(3, null, MemoryMarshal.Cast<float, byte>(doc.Vector), random);
            builder.EndWriting();
        }

        indexWriter.Commit();
    }
    
    private record VectorSearchResult(string Id, float Distance, float[] Vector);

    private record Doc(string Id, string Text, int Numerical, float[] Vector);
}
