using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Lucene.Net.Analysis;
using Lucene.Net.Search;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
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

        protected static RavenPerFieldAnalyzerWrapper CreateAnalyzer(IndexingConfiguration configuration, IndexDefinitionBase indexDefinition, bool forQuerying = false)
        {
            if (indexDefinition.IndexFields.ContainsKey(Constants.Documents.Indexing.Fields.AllFields))
                throw new InvalidOperationException($"Detected '{Constants.Documents.Indexing.Fields.AllFields}'. This field should not be present here, because inheritance is done elsewhere.");

            var analyzers = new Dictionary<Type, Analyzer>();

            var hasDefaultFieldOptions = false;
            Analyzer defaultAnalyzerToUse = null;
            Analyzer defaultAnalyzer = null;
            if (indexDefinition is MapIndexDefinition mid)
            {
                if (mid.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out var value))
                {
                    hasDefaultFieldOptions = true;

                    switch (value.Indexing)
                    {
                        case FieldIndexing.Exact:
                            defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields, configuration.DefaultExactAnalyzerType, CreateKeywordAnalyzer);
                            break;

                        case FieldIndexing.Search:
                            if (value.Analyzer != null)
                                defaultAnalyzerToUse = GetAnalyzer(Constants.Documents.Indexing.Fields.AllFields, value.Analyzer, analyzers, forQuerying);

                            if (defaultAnalyzerToUse == null)
                                defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields, configuration.DefaultSearchAnalyzerType, CreateStandardAnalyzer);
                            break;

                        default:
                            // explicitly ignore all other values
                            break;
                    }
                }
            }

            if (defaultAnalyzerToUse == null)
            {
                defaultAnalyzerToUse = defaultAnalyzer = CreateDefaultAnalyzer(Constants.Documents.Indexing.Fields.AllFields, configuration.DefaultAnalyzerType);
                analyzers.Add(defaultAnalyzerToUse.GetType(), defaultAnalyzerToUse);
            }

            var perFieldAnalyzerWrapper = forQuerying == false && indexDefinition.HasDynamicFields
                ? new RavenPerFieldAnalyzerWrapper(
                        defaultAnalyzerToUse,
                        fieldName => GetOrCreateAnalyzer(fieldName, configuration.DefaultSearchAnalyzerType, CreateStandardAnalyzer),
                        fieldName => GetOrCreateAnalyzer(fieldName, configuration.DefaultExactAnalyzerType, CreateKeywordAnalyzer))
                : new RavenPerFieldAnalyzerWrapper(defaultAnalyzerToUse);

            foreach (var field in indexDefinition.IndexFields)
            {
                var fieldName = field.Value.Name;

                switch (field.Value.Indexing)
                {
                    case FieldIndexing.Exact:
                        var keywordAnalyzer = GetOrCreateAnalyzer(fieldName, configuration.DefaultExactAnalyzerType, CreateKeywordAnalyzer);

                        perFieldAnalyzerWrapper.AddAnalyzer(fieldName, keywordAnalyzer);
                        break;

                    case FieldIndexing.Search:
                        var analyzer = GetAnalyzer(fieldName, field.Value.Analyzer, analyzers, forQuerying);
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
                                defaultAnalyzer = CreateDefaultAnalyzer(fieldName, configuration.DefaultAnalyzerType);

                            perFieldAnalyzerWrapper.AddAnalyzer(fieldName, defaultAnalyzer);
                            continue;
                        }
                        break;
                }
            }

            return perFieldAnalyzerWrapper;

            void AddStandardAnalyzer(string fieldName)
            {
                var standardAnalyzer = GetOrCreateAnalyzer(fieldName, configuration.DefaultSearchAnalyzerType, CreateStandardAnalyzer);

                perFieldAnalyzerWrapper.AddAnalyzer(fieldName, standardAnalyzer);
            }

            Analyzer GetOrCreateAnalyzer(string fieldName, Type analyzerType, Func<string, Type, Analyzer> createAnalyzer)
            {
                if (analyzers.TryGetValue(analyzerType, out var analyzer) == false)
                    analyzers[analyzerType] = analyzer = createAnalyzer(fieldName, analyzerType);

                return analyzer;
            }

            Analyzer CreateDefaultAnalyzer(string fieldName, Type analyzerType)
            {
                if (analyzerType == typeof(LowerCaseKeywordAnalyzer))
                    return new LowerCaseKeywordAnalyzer();

                return IndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
            }

            Analyzer CreateKeywordAnalyzer(string fieldName, Type analyzerType)
            {
                if (analyzerType == typeof(KeywordAnalyzer))
                    return new KeywordAnalyzer();

                return IndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
            }

            Analyzer CreateStandardAnalyzer(string fieldName, Type analyzerType)
            {
                if (analyzerType == typeof(RavenStandardAnalyzer))
                    return new RavenStandardAnalyzer(Version.LUCENE_29);

                return IndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
            }
        }

        public abstract void Dispose();

        private static Analyzer GetAnalyzer(string name, string analyzer, Dictionary<Type, Analyzer> analyzers, bool forQuerying)
        {
            if (string.IsNullOrWhiteSpace(analyzer))
                return null;

            var analyzerType = IndexingExtensions.GetAnalyzerType(name, analyzer);

            if (forQuerying)
            {
                var notForQuerying = NotForQuerying
                    .GetOrAdd(analyzerType, t => t.GetCustomAttributes<NotForQueryingAttribute>(false).Any());

                if (notForQuerying)
                    return null;
            }

            if (analyzers.TryGetValue(analyzerType, out var analyzerInstance) == false)
                analyzers[analyzerType] = analyzerInstance = IndexingExtensions.CreateAnalyzerInstance(name, analyzerType);

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
                            documentQuery = QueryBuilder.BuildQuery(serverContext, context, metadata, whereExpression, _index, parameters, analyzer, factories);
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
