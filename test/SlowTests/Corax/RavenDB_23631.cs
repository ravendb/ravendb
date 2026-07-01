using System;
using System.Collections.Generic;
using System.Linq;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;
using Raven.Server.Documents.Queries;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;


namespace SlowTests.Corax;

public class RavenDB_23631(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void MultiTermMatchDoesNotReturnDuplicatesWhenPerformingAndWith()
    {
        using var mapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, "docId")
            .AddBinding(1, "name")
            .Build();

        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (int i = 0; i < 1000; i++)
            {
                using (var builder = writer.Index($"id/{i}"))
                {
                    builder.Write(0, Encodings.Utf8.GetBytes($"id/{i}"));
                    builder.IncrementList();
                    builder.Write(1, Encodings.Utf8.GetBytes("name/0"));
                    builder.Write(1, Encodings.Utf8.GetBytes("name/1"));
                    builder.DecrementList();
                    builder.EndWriting();
                }
            }

            writer.Commit();
        }

        // AND of an IN query with a list-valued field must not produce duplicate entry IDs.
        var results = ExecuteRQLQuery(mapping, "FROM TestIndex WHERE docId IN ('id/0', 'id/10')");
        Assert.Equal(2, results.Count);
        Assert.Equal(results.Distinct().Count(), results.Count);
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void MultiTermMatchProperlyHandlesDuplicatesWhenPerformingAndWith()
    {
        using var mapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, "docId")
            .AddBinding(1, "name")
            .Build();

        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (int i = 0; i < 20; i++)
            {
                using (var builder = writer.Index($"id/{i}"))
                {
                    builder.Write(0, Encodings.Utf8.GetBytes($"id/{i}"));
                    builder.IncrementList();
                    builder.Write(1, Encodings.Utf8.GetBytes("name/0"));
                    builder.Write(1, Encodings.Utf8.GetBytes("name/1"));
                    builder.DecrementList();
                    builder.EndWriting();
                }
            }

            writer.Commit();
        }

        var inTerms = Enumerable.Range(0, 10).Select(i => $"id/{i}").ToList();
        var inClause = string.Join(", ", inTerms.Select(t => $"'{t}'"));

        // AND of an IN query (10 terms) with a list-valued field must not produce duplicate entry IDs.
        var results = ExecuteRQLQuery(mapping, $"FROM TestIndex WHERE docId IN ({inClause})");
        Assert.Equal(10, results.Count);
        Assert.Equal(results.Distinct().Count(), results.Count);
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void InQueryAndExistsOnListFieldDoesNotProduceDuplicates()
    {
        // This is the exact scenario from the original RavenDB-23631 bug:
        // AND of InQuery with ExistsQuery on a list-valued field (multiple values per doc).
        // The list field produces multiple postings per document in the CompactTree,
        // which could cause duplicate entry IDs in the intersection result.
        using var mapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, "docId")
            .AddBinding(1, "name")
            .Build();

        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (int i = 0; i < 1000; i++)
            {
                using (var builder = writer.Index($"id/{i}"))
                {
                    builder.Write(0, Encodings.Utf8.GetBytes($"id/{i}"));
                    builder.IncrementList();
                    builder.Write(1, Encodings.Utf8.GetBytes("name/0"));
                    builder.Write(1, Encodings.Utf8.GetBytes("name/1"));
                    builder.DecrementList();
                    builder.EndWriting();
                }
            }

            writer.Commit();
        }

        // IN(docId, ['id/0', 'id/10']) AND exists(name)
        // The exists on a list field with 2 values per doc must not produce duplicates.
        var results = ExecuteRQLQuery(mapping, "FROM TestIndex WHERE docId IN ('id/0', 'id/10') AND exists(name)");
        Assert.Equal(2, results.Count);
        Assert.Equal(results.Distinct().Count(), results.Count);

        // Larger IN set + exists
        var inTerms = Enumerable.Range(0, 10).Select(i => $"id/{i}").ToList();
        var inClause = string.Join(", ", inTerms.Select(t => $"'{t}'"));
        results = ExecuteRQLQuery(mapping, $"FROM TestIndex WHERE docId IN ({inClause}) AND exists(name)");
        Assert.Equal(10, results.Count);
        Assert.Equal(results.Distinct().Count(), results.Count);
    }

    private List<string> ExecuteRQLQuery(IndexFieldsMapping mapping, string rqlQuery)
    {
        using var searcher = new IndexSearcher(Env, mapping);
        var queryMetadata = new QueryMetadata(rqlQuery, null, 0);
        var planParams = new PlanParameters
        {
            IndexSearcher = searcher,
            Metadata = queryMetadata,
            QueryParameters = null,
            Allocator = Allocator
        };
        var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, null, mapping), null, false, default);
        var list = new List<string>();
        Span<long> ids = stackalloc long[256];
        int count;
        while ((count = match.Fill(ids)) > 0)
            for (int i = 0; i < count; i++)
                list.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(ids[i]));
        return list;
    }
}
