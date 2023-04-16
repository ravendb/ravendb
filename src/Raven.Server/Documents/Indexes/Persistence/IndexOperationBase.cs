using System;
using System.Runtime.CompilerServices;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Search;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Query = Lucene.Net.Search.Query;
using LuceneAnalyzer = Lucene.Net.Analysis.Analyzer;
using CoraxAnalyzer = Corax.Analyzer;

namespace Raven.Server.Documents.Indexes.Persistence;

public abstract class IndexOperationBase : IDisposable
{
    protected readonly string _indexName;
    private const int DefaultBufferSizeForCorax = 4 * 1024;
    private const int MaxBufferSizeForCorax = 64 * 1024;

    protected readonly Logger _logger;
    internal Index _index;

    protected IndexOperationBase(Index index, Logger logger)
    {
        _index = index;
        _indexName = index.Name;
        _logger = logger;
    }

    public abstract void Dispose();

    protected Query GetLuceneQuery(DocumentsOperationContext context, QueryMetadata metadata, BlittableJsonReaderObject parameters, Analyzer analyzer,
        QueryBuilderFactories factories)
    {
        return GetLuceneQuery(context, metadata, metadata.Query.Where, parameters, analyzer, factories);
    }

    protected Query GetLuceneQuery(DocumentsOperationContext context, QueryMetadata metadata, QueryExpression whereExpression, BlittableJsonReaderObject parameters,
        Analyzer analyzer, QueryBuilderFactories factories)
    {
        Query documentQuery;

        if (metadata.Query.Where == null)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Issuing query on index {_indexName} for all documents");

            documentQuery = new MatchAllDocsQuery();
        }
        else
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Issuing query on index {_indexName} for: {metadata.Query}");

            // RavenPerFieldAnalyzerWrapper searchAnalyzer = null;
            try
            {
                //_persistence._a
                //searchAnalyzer = parent.CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose, true);
                //searchAnalyzer = parent.AnalyzerGenerators.Aggregate(searchAnalyzer, (currentAnalyzer, generator) =>
                //{
                //    Analyzer newAnalyzer = generator.GenerateAnalyzerForQuerying(parent.PublicName, query.Query, currentAnalyzer);
                //    if (newAnalyzer != currentAnalyzer)
                //    {
                //        DisposeAnalyzerAndFriends(toDispose, currentAnalyzer);
                //    }
                //    return parent.CreateAnalyzer(newAnalyzer, toDispose, true);
                //});

                IDisposable releaseServerContext = null;
                IDisposable closeServerTransaction = null;
                TransactionOperationContext serverContext = null;

                try
                {
                    if (metadata.HasCmpXchg)
                    {
                        releaseServerContext = context.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out serverContext);
                        closeServerTransaction = serverContext.OpenReadTransaction();
                    }

                    using (closeServerTransaction)
                        documentQuery = LuceneQueryBuilder.BuildQuery(serverContext, context, metadata, whereExpression, _index, parameters, analyzer, factories);
                }
                finally
                {
                    releaseServerContext?.Dispose();
                }
            }
            finally
            {
                //DisposeAnalyzerAndFriends(toDispose, searchAnalyzer);
            }
        }

        //var afterTriggers = ApplyIndexTriggers(documentQuery);

        return documentQuery;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static int LuceneGetPageSize(IndexSearcher searcher, long pageSize)
    {
        if (pageSize >= searcher.MaxDoc)
            return searcher.MaxDoc;

        if (pageSize >= int.MaxValue)
            return int.MaxValue;

        return (int)pageSize;
    }

    protected static int CoraxBufferSize(global::Corax.IndexSearcher searcher, long pageSize, IndexQueryServerSide query)
    {
        var numberOfEntries = searcher.NumberOfEntries;
        if (numberOfEntries == 0)
            return 16;
        
        if (query.Metadata.OrderBy is not null || query.Metadata.IsDistinct)
        {
            if (numberOfEntries > MaxBufferSizeForCorax)
                return MaxBufferSizeForCorax;
            
            return DefaultBufferSizeForCorax;
        }
        
        if (pageSize <= 0 && numberOfEntries < DefaultBufferSizeForCorax)
            return DefaultBufferSizeForCorax;
        
        return numberOfEntries > MaxBufferSizeForCorax
            ? MaxBufferSizeForCorax
            : DefaultBufferSizeForCorax;
    }
    
    protected QueryFilter GetQueryFilter(Index index, IndexQueryServerSide query, DocumentsOperationContext documentsContext, Reference<int> skippedResults,
        Reference<int> scannedDocuments, IQueryResultRetriever retriever, QueryTimingsScope queryTimings)

    {
        if (query.Metadata.FilterScript is null)
            return null;

        return new QueryFilter(index, query, documentsContext, skippedResults, scannedDocuments, retriever, queryTimings);
    }
    
    internal static unsafe BlittableJsonReaderObject ParseJsonStringIntoBlittable(string json, JsonOperationContext context)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        fixed (byte* ptr = bytes)
        {
            var blittableJson = context.ParseBuffer(ptr, bytes.Length, "MoreLikeThis/ExtractTermsFromJson", BlittableJsonDocumentBuilder.UsageMode.None);
            blittableJson.BlittableValidation(); //precaution, needed because this is user input..
            return blittableJson;
        }
    }
}
