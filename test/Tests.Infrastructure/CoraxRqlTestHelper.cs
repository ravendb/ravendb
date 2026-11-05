using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Tests.Infrastructure;

/// <summary>
/// Shared helper for direct-IndexSearcher Corax tests that parse an RQL string, build the query plan via
/// QueryPlanBuilder.BuildFilterMatch, execute it and drain the results. Consolidates the near-identical
/// ExecuteRQLQuery/ExecuteRQLQueryByScore helpers that used to be duplicated across several test classes.
/// </summary>
public static class CoraxRqlTestHelper
{
    /// <summary>
    /// Builds and runs the RQL query, returning the matching entry ids as-is (no score ordering).
    /// </summary>
    public static List<long> ExecuteRQLQuery(IndexSearcher searcher, ByteStringContext allocator, IndexFieldsMapping knownFields, string rqlQuery,
        BlittableJsonReaderObject queryParameters = null)
    {
        var queryMetadata = new QueryMetadata(rqlQuery, null, 0);
        var match = BuildMatch(searcher, allocator, knownFields, queryMetadata, queryParameters, hasBoost: false);
        return Drain(match);
    }

    /// <summary>
    /// Same as <see cref="ExecuteRQLQuery(IndexSearcher, ByteStringContext, IndexFieldsMapping, string, BlittableJsonReaderObject)"/>
    /// but resolves each entry id to its document id via the first indexed field.
    /// </summary>
    public static List<string> ExecuteRQLQueryAsDocumentIds(IndexSearcher searcher, ByteStringContext allocator, IndexFieldsMapping knownFields, string rqlQuery,
        BlittableJsonReaderObject queryParameters = null)
    {
        var entryIds = ExecuteRQLQuery(searcher, allocator, knownFields, rqlQuery, queryParameters);
        return ResolveTerms(searcher, searcher.GetFirstIndexedFiledName(), entryIds);
    }

    /// <summary>
    /// Builds and runs the RQL query with boosting/scoring enabled, applying score-ordering when the query
    /// has an ORDER BY score() clause, then resolves each entry id to its document id via the first indexed field.
    /// </summary>
    public static List<string> ExecuteRQLQueryByScore(IndexSearcher searcher, ByteStringContext allocator, IndexFieldsMapping knownFields, string rqlQuery,
        long take = long.MaxValue)
    {
        var entryIds = ExecuteRQLQueryByScoreCore(searcher, allocator, knownFields, rqlQuery, take);
        return ResolveTerms(searcher, searcher.GetFirstIndexedFiledName(), entryIds);
    }

    /// <summary>
    /// Same as <see cref="ExecuteRQLQueryByScore"/> but resolves each entry id to a value read from
    /// <paramref name="fieldName"/> (via its TermsReader) and parsed as a long.
    /// </summary>
    public static List<long> ExecuteRQLQueryByScoreReadField(IndexSearcher searcher, ByteStringContext allocator, IndexFieldsMapping knownFields, string rqlQuery,
        string fieldName, long take = long.MaxValue)
    {
        var entryIds = ExecuteRQLQueryByScoreCore(searcher, allocator, knownFields, rqlQuery, take);
        var termsReader = searcher.TermsReaderFor(fieldName);

        var results = new List<long>(entryIds.Count);
        foreach (long id in entryIds)
            results.Add(long.Parse(termsReader.GetTermFor(id)));

        return results;
    }

    private static List<long> ExecuteRQLQueryByScoreCore(IndexSearcher searcher, ByteStringContext allocator, IndexFieldsMapping knownFields, string rqlQuery, long take)
    {
        var queryMetadata = new QueryMetadata(rqlQuery, null, 0);
        var match = BuildMatch(searcher, allocator, knownFields, queryMetadata, queryParameters: null, hasBoost: true);
        match = ApplyScoreOrderingIfRequested(searcher, queryMetadata, match, take);
        return Drain(match);
    }

    private static IQueryMatch BuildMatch(IndexSearcher searcher, ByteStringContext allocator, IndexFieldsMapping knownFields, QueryMetadata queryMetadata,
        BlittableJsonReaderObject queryParameters, bool hasBoost)
    {
        var planParams = new PlanParameters
        {
            IndexSearcher = searcher,
            Metadata = queryMetadata,
            QueryParameters = queryParameters,
            HasBoost = hasBoost,
            Allocator = allocator
        };

        return QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, allocator, queryMetadata, queryParameters, knownFields, hasBoost),
            highlightingTerms: null, wantTimings: false, CancellationToken.None);
    }

    // Wraps the searcher's score-ordering primitive directly. Production OrderBy(QueryBuilderParameters, ...)
    // needs the full server-side query pipeline that these direct-IndexSearcher tests bypass.
    private static IQueryMatch ApplyScoreOrderingIfRequested(IndexSearcher searcher, QueryMetadata queryMetadata, IQueryMatch match, long take)
    {
        var orderByFields = queryMetadata.OrderBy;
        if (orderByFields is null || orderByFields.Length == 0)
            return match;

        int takeInt = take > int.MaxValue ? Corax.Constants.IndexSearcher.TakeAll : (int)take;
        foreach (var field in orderByFields)
        {
            if (field.OrderingType == OrderByFieldType.Score)
            {
                var meta = new OrderMetadata(true, MatchCompareFieldType.Score, field.Ascending);
                return searcher.OrderBy(match, meta, NullsSortMode.NullsLargest, take: takeInt);
            }
        }

        return match;
    }

    private static List<long> Drain(IQueryMatch match)
    {
        var results = new List<long>();
        Span<long> buffer = stackalloc long[256];
        int count;
        while ((count = match.Fill(buffer)) > 0)
        {
            for (int i = 0; i < count; i++)
                results.Add(buffer[i]);
        }

        return results;
    }

    private static List<string> ResolveTerms(IndexSearcher searcher, Slice fieldName, List<long> entryIds)
    {
        var termsReader = searcher.TermsReaderFor(fieldName);
        var results = new List<string>(entryIds.Count);
        foreach (long id in entryIds)
            results.Add(termsReader.GetTermFor(id));

        return results;
    }
}
