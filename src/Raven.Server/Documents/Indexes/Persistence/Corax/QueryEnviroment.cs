using System.Collections.Generic;
using Corax;
using Corax.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.IndexSearcher;
using CoraxConstants = Corax.Constants;
using MoreLikeThisQuery = Raven.Server.Documents.Queries.MoreLikeThis.Corax;
namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal class QueryParameters
{
    public readonly IndexSearcher IndexSearcher;
    public readonly TransactionOperationContext ServerContext;
    public readonly DocumentsOperationContext DocumentsContext;
    public readonly IndexQueryServerSide Query;
    public readonly Index Index;
    public readonly BlittableJsonReaderObject Parameters;
    public readonly QueryBuilderFactories Factories;
    public readonly IndexFieldsMapping IndexFieldsMapping;
    public readonly FieldsToFetch FieldsToFetch;
    public readonly Dictionary<string, CoraxHighlightingTermIndex> HighlightingTerms;
    public readonly int Take;
    public readonly List<string> BuildSteps;
    public readonly MemoizationMatchProvider<AllEntriesMatch> AllEntries;
    public readonly QueryMetadata Metadata;
    
    internal QueryParameters(IndexSearcher searcher, TransactionOperationContext serverContext, DocumentsOperationContext documentsContext, IndexQueryServerSide query, Index index, BlittableJsonReaderObject parameters, QueryBuilderFactories factories, IndexFieldsMapping indexFieldsMapping, FieldsToFetch fieldsToFetch, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, int take, List<string> buildSteps = null)
    {
        IndexSearcher = searcher;
        ServerContext = serverContext;
        Query = query;
        Index = index;
        Parameters = parameters;
        Factories = factories;
        IndexFieldsMapping = indexFieldsMapping;
        FieldsToFetch = fieldsToFetch;
        DocumentsContext = documentsContext;
        HighlightingTerms = highlightingTerms;
        Take = take;
        BuildSteps = buildSteps;
        AllEntries = IndexSearcher.Memoize(IndexSearcher.AllEntries());
        Metadata = query.Metadata;
    }
}
