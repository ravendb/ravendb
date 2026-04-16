using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public partial class CoraxQueryBuilder
{
    public sealed class Parameters
    {
        public readonly IndexSearcher IndexSearcher;
        public readonly TransactionOperationContext ServerContext;
        public readonly DocumentsOperationContext DocumentsContext;
        public readonly IndexQueryServerSide Query;
        public readonly Index Index;
        public readonly BlittableJsonReaderObject QueryParameters;
        public readonly QueryBuilderFactories Factories;
        public readonly IndexFieldsMapping IndexFieldsMapping;
        public readonly FieldsToFetch FieldsToFetch;
        public readonly Dictionary<string, CoraxHighlightingTermIndex> HighlightingTerms;
        public readonly int Take;
        public readonly CancellationToken Token;
        public readonly List<string> BuildSteps;
        public readonly MemoizationMatchProvider<AllEntriesMatch> AllEntries;
        public readonly QueryMetadata Metadata;
        public readonly bool HasDynamics;
        public readonly Lazy<List<string>> DynamicFields;
        public readonly ByteStringContext Allocator;
        public readonly bool HasBoost;
        public readonly bool DeduplicationDisabled;
        public readonly IndexReadOperationBase IndexReadOperation;
        public StreamingOptimization StreamingDisabled;
        public readonly bool IsVectorSingleClause;
        public readonly QueryTimeScope QueryTime;

        internal Parameters(IndexSearcher searcher, ByteStringContext allocator, TransactionOperationContext serverContext, DocumentsOperationContext documentsContext,
            IndexQueryServerSide query, Index index, BlittableJsonReaderObject queryParameters, QueryBuilderFactories factories, IndexFieldsMapping indexFieldsMapping,
            FieldsToFetch fieldsToFetch, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, int take, bool deduplicationDisabled, IndexReadOperationBase indexReadOperation = null, List<string> buildSteps = null, QueryTimeScope queryTime = null, CancellationToken token = default)
        {
            QueryTime = queryTime;
            IndexSearcher = searcher;
            ServerContext = serverContext;
            Query = query;
            Index = index;
            QueryParameters = queryParameters;
            Factories = factories;
            IndexFieldsMapping = indexFieldsMapping;
            FieldsToFetch = fieldsToFetch;
            DocumentsContext = documentsContext;
            HighlightingTerms = highlightingTerms;
            Take = take;
            Token = token;
            BuildSteps = buildSteps;
            AllEntries = IndexSearcher.Memoize(IndexSearcher.AllEntries());
            Metadata = query.Metadata;
            HasDynamics = index.Definition.HasDynamicFields;
            IsVectorSingleClause = Metadata.Query.Where is MethodExpression me && QueryMethod.GetMethodType(me.Name.Value) == MethodType.Vector_Search && Metadata.OrderBy is null or {Length: 0};
            DynamicFields = HasDynamics
                ? new Lazy<List<string>>(() => IndexSearcher.GetFields())
                : null;

            // in case when we've implicit boosting we've built primitives with scoring enabled
            HasBoost = index.HasBoostedFields
                       || query.Metadata.HasBoost
                       || IsVectorSingleClause
                       || (query.Metadata.HasVectorSearch && index.Configuration.CoraxVectorSearchOrderByScoreAutomatically)
                       || HasBoostingAsOrderingType(query.Metadata.OrderBy);
            Allocator = allocator;
            IndexReadOperation = indexReadOperation;
            DeduplicationDisabled = deduplicationDisabled;
        }
        
        public bool NeedsScoresBuffer() => HasBoost
            && (Index.Configuration.CoraxIncludeDocumentScore || (IndexReadOperation.IsSharded && Metadata.HasVectorSearch));

        private static bool HasBoostingAsOrderingType(OrderByField[] orderBy)
        {
            if (orderBy is null)
                return false;

            foreach (var field in orderBy)
                if (field.OrderingType == OrderByFieldType.Score)
                    return true;

            return false;
        }
    }
}
