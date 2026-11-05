using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27433(ITestOutputHelper output) : RavenTestBase(output)
{
    private sealed class VecDoc
    {
        public string Id { get; set; }
        public float[] Embedding { get; set; }
    }

    private sealed class VecIndex : AbstractIndexCreationTask<VecDoc>
    {
        public VecIndex()
        {
            Map = docs => from d in docs
                          select new
                          {
                              Embedding = CreateVector(d.Embedding)
                          };
        }
    }

    private const int DocCount = 32;

    // Query vector points east. The seeded unit vectors fan out over the first quadrant, so cosine similarity to
    // the query decreases monotonically with the doc number. No assertion here depends on the HNSW graph layout.
    private static readonly float[] QueryVector = [1f, 0f];

    private IDocumentStore SetupStore()
    {
        var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        using (var session = store.OpenSession())
        {
            for (int i = 0; i < DocCount; i++)
            {
                double angle = i * (Math.PI / 2) / DocCount; // 0 .. <90 degrees
                session.Store(new VecDoc { Id = $"docs/{i}", Embedding = [(float)Math.Cos(angle), (float)Math.Sin(angle)] });
            }

            session.SaveChanges();
        }

        new VecIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        return store;
    }

    // The vector match is the only plan node reporting IsExact, so this finds it for both the single-vector
    // (VectorSearchMatch) and the multi-vector (MultiVectorSearchMatch) shapes.
    private static QueryInspectionNode FindVectorNode(QueryInspectionNode node)
    {
        if (node is null)
            return null;
        if (node.Parameters is not null && node.Parameters.ContainsKey("IsExact"))
            return node;
        foreach (var child in node.Children ?? [])
        {
            var hit = FindVectorNode(child);
            if (hit != null)
                return hit;
        }

        return null;
    }

    private (List<string> Ids, QueryInspectionNode Plan) Query(IDocumentStore store, bool exact, object vector)
    {
        string predicate = "vector.search(Embedding, $vec, $minSim, $candidates)";
        string rql = $@"from index 'VecIndex'
                        where {(exact ? $"exact({predicate})" : predicate)}
                        include timings()";

        using var session = store.OpenSession();
        var ids = session.Advanced
            .RawQuery<VecDoc>(rql)
            .AddParameter("vec", vector)
            .AddParameter("minSim", 0.5f)
            // More candidates than there are documents, so an exact scan returns every doc above minSim.
            .AddParameter("candidates", DocCount * 4)
            .Timings(out var timings)
            .WaitForNonStaleResults()
            .ToList()
            .Select(d => d.Id)
            .ToList();

        var vectorNode = FindVectorNode(timings.QueryPlan as QueryInspectionNode);
        Assert.NotNull(vectorNode);

        return (ids, vectorNode);
    }

    private static long ScannedCandidates(QueryInspectionNode vectorNode) =>
        long.Parse(vectorNode.Parameters["NumberOfCandidatesScanned"], NumberStyles.AllowThousands, CultureInfo.InvariantCulture);

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void ExactVectorSearchMustBruteForceAndNotFallBackToApproximateSearch(bool approximateFirst)
    {
        using var store = SetupStore();

        // Both orders are exercised on purpose: exact() is part of the structural plan key, so whichever form runs
        // first must not hand its compiled plan (and its search mode) to the other.
        var approximate = approximateFirst ? AssertApproximate() : default;

        var exact = Query(store, exact: true, QueryVector);
        Assert.Equal("True", exact.Plan.Parameters["IsExact"]);
        Assert.Equal("ExactAll", exact.Plan.Parameters["SearchMode"]);
        // Brute force means every indexed vector is enumerated, not just the nodes an HNSW walk happens to reach.
        Assert.Equal(DocCount, ScannedCandidates(exact.Plan));
        Assert.NotEmpty(exact.Ids);

        if (approximateFirst == false)
            approximate = AssertApproximate();

        // numberOfCandidates exceeds the document count, so the exact scan returns every match above minSim -
        // the approximate result set can only ever be a subset of it.
        Assert.Empty(approximate.Ids.Except(exact.Ids));

        (List<string> Ids, QueryInspectionNode Plan) AssertApproximate()
        {
            var result = Query(store, exact: false, QueryVector);
            Assert.Equal("False", result.Plan.Parameters["IsExact"]);
            Assert.Equal("ApproximateAll", result.Plan.Parameters["SearchMode"]);
            return result;
        }
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void ExactMultiVectorSearchMustBruteForce()
    {
        using var store = SetupStore();

        float[][] vectors = [[1f, 0f], [0f, 1f]];

        Assert.Equal("False", Query(store, exact: false, vectors).Plan.Parameters["IsExact"]);
        Assert.Equal("True", Query(store, exact: true, vectors).Plan.Parameters["IsExact"]);
    }
}
