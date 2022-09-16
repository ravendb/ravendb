using System.Collections.Generic;
using Corax;
using Corax.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.IndexSearcher;
using CoraxConstants = Corax.Constants;
using MoreLikeThisQuery = Raven.Server.Documents.Queries.MoreLikeThis.CoraxMoreLikeThisQuery;
namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal readonly struct QueryEnvironment
{
    internal readonly IndexSearcher IndexSearcher;
    internal readonly TransactionOperationContext ServerContext;
    internal readonly DocumentsOperationContext Context;
    internal readonly IndexQueryServerSide Query;
    internal readonly Index Index;
    internal readonly BlittableJsonReaderObject Parameters;
    internal readonly QueryBuilderFactories Factories;
    internal readonly IndexFieldsMapping IndexFieldsMapping;
    internal readonly FieldsToFetch FieldsToFetch;
    internal readonly Dictionary<string, CoraxHighlightingTermIndex> HighlightingTerms;
    internal readonly int Take;
    internal readonly List<string> BuildSteps;
    internal readonly MemoizationMatchProvider<AllEntriesMatch> AllEntries;
    internal readonly QueryMetadata Metadata;
    
    internal QueryEnvironment(IndexSearcher searcher, TransactionOperationContext serverContext, DocumentsOperationContext context, IndexQueryServerSide query, Index index, BlittableJsonReaderObject parameters, QueryBuilderFactories factories, IndexFieldsMapping indexFieldsMapping, FieldsToFetch fieldsToFetch, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, int take, List<string> buildSteps = null)
    {
        IndexSearcher = searcher;
        ServerContext = serverContext;
        Query = query;
        Index = index;
        Parameters = parameters;
        Factories = factories;
        IndexFieldsMapping = indexFieldsMapping;
        FieldsToFetch = fieldsToFetch;
        Context = context;
        HighlightingTerms = highlightingTerms;
        Take = take;
        BuildSteps = buildSteps;
        AllEntries = IndexSearcher.Memoize(IndexSearcher.AllEntries());
        Metadata = query.Metadata;
    }
}
