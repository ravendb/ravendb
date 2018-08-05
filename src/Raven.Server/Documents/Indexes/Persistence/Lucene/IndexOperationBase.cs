using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Lucene.Net.Analysis;
using Lucene.Net.Search;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Query = Lucene.Net.Search.Query;
using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public abstract class IndexOperationBase : IDisposable
    {
        private static readonly ConcurrentDictionary<Type, bool> NotForQuerying = new ConcurrentDictionary<Type, bool>();

        protected readonly string _indexName;

        protected readonly Logger _logger;
        internal Index _index;

        protected IndexOperationBase(Index index, Logger logger)
        {
            _index = index;
            _indexName = index.Name;
            _logger = logger;
        }

        protected static RavenPerFieldAnalyzerWrapper CreateAnalyzer(Func<Analyzer> createDefaultAnalyzer, IndexDefinitionBase indexDefinition, bool forQuerying = false)
        {
            if (indexDefinition.IndexFields.ContainsKey(Constants.Documents.Indexing.Fields.AllFields))
                throw new InvalidOperationException($"Detected '{Constants.Documents.Indexing.Fields.AllFields}'. This field should not be present here, because inheritance is done elsewhere.");

            var hasDefaultFieldOptions = false;
            Analyzer defaultAnalyzerToUse = null;
            RavenStandardAnalyzer standardAnalyzer = null;
            KeywordAnalyzer keywordAnalyzer = null;
            Analyzer defaultAnalyzer = null;
            if (indexDefinition is MapIndexDefinition mid)
            {
                if (mid.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out var value))
                {
                    hasDefaultFieldOptions = true;

                    switch (value.Indexing)
                    {
                        case FieldIndexing.Exact:
                            defaultAnalyzerToUse = keywordAnalyzer = new KeywordAnalyzer();
                            break;
                        case FieldIndexing.Search:
                            if (value.Analyzer != null)
                                defaultAnalyzerToUse = GetAnalyzer(Constants.Documents.Indexing.Fields.AllFields, value.Analyzer, forQuerying);
                            if (defaultAnalyzerToUse == null)
                                defaultAnalyzerToUse = standardAnalyzer = new RavenStandardAnalyzer(Version.LUCENE_29);
                            break;
                        default:
                            // explicitly ignore all other values
                            break;
                    }
                }
            }

            if (defaultAnalyzerToUse == null)
                defaultAnalyzerToUse = defaultAnalyzer = createDefaultAnalyzer();

            var perFieldAnalyzerWrapper = new RavenPerFieldAnalyzerWrapper(defaultAnalyzerToUse);
            foreach (var field in indexDefinition.IndexFields)
            {
                var fieldName = field.Value.Name;

                switch (field.Value.Indexing)
                {
                    case FieldIndexing.Exact:
                        if (keywordAnalyzer == null)
                            keywordAnalyzer = new KeywordAnalyzer();

                        perFieldAnalyzerWrapper.AddAnalyzer(fieldName, keywordAnalyzer);
                        break;
                    case FieldIndexing.Search:
                        var analyzer = GetAnalyzer(fieldName, field.Value.Analyzer, forQuerying);
                        if (analyzer != null)
                        {
                            perFieldAnalyzerWrapper.AddAnalyzer(fieldName, analyzer);
                            continue;
                        }
                        AddStandardAnalyzer(fieldName);
                        break;
                    case FieldIndexing.Default:
                        if (hasDefaultFieldOptions)
                        {
                            // if we have default field options then we need to take into account overrides for regular fields

                            if (defaultAnalyzer == null)
                                defaultAnalyzer = createDefaultAnalyzer();

                            perFieldAnalyzerWrapper.AddAnalyzer(fieldName, defaultAnalyzer);
                            continue;
                        }
                        break;
                }
            }

            return perFieldAnalyzerWrapper;

            void AddStandardAnalyzer(string fieldName)
            {
                if (standardAnalyzer == null)
                    standardAnalyzer = new RavenStandardAnalyzer(Version.LUCENE_29);

                perFieldAnalyzerWrapper.AddAnalyzer(fieldName, standardAnalyzer);
            }
        }

        public abstract void Dispose();

        private static Analyzer GetAnalyzer(string name, string analyzer, bool forQuerying)
        {
            if (string.IsNullOrWhiteSpace(analyzer))
                return null;

            var analyzerInstance = IndexingExtensions.CreateAnalyzerInstance(name, analyzer);

            if (forQuerying)
            {
                var analyzerType = analyzerInstance.GetType();

                var notForQuerying = NotForQuerying
                    .GetOrAdd(analyzerType, t => analyzerInstance.GetType().GetTypeInfo().GetCustomAttributes<NotForQueryingAttribute>(false).Any());

                if (notForQuerying)
                    return null;
            }

            return analyzerInstance;
        }

        protected Query GetLuceneQuery(DocumentsOperationContext context, QueryMetadata metadata, BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories)
        {
            return GetLuceneQuery(context, metadata, metadata.Query.Where, parameters, analyzer, factories);
        }

        protected Query GetLuceneQuery(DocumentsOperationContext context, QueryMetadata metadata, QueryExpression whereExpression, BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories)
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
                            documentQuery = QueryBuilder.BuildQuery(serverContext, context, metadata, whereExpression, _index.Definition, parameters, analyzer, factories);
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
        protected static int GetPageSize(IndexSearcher searcher, long pageSize)
        {
            if (pageSize >= searcher.MaxDoc)
                return searcher.MaxDoc;

            if (pageSize >= int.MaxValue)
                return int.MaxValue;

            return (int)pageSize;
        }
    }
}
