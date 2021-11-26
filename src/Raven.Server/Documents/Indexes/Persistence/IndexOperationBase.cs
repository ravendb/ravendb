using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Corax.Pipeline;
using Lucene.Net.Analysis;
using Lucene.Net.Search;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using KeywordTokenizer = Corax.Pipeline.KeywordTokenizer;
using Query = Lucene.Net.Search.Query;
using Version = Lucene.Net.Util.Version;
using WhitespaceTokenizer = Corax.Pipeline.WhitespaceTokenizer;
using LuceneAnalyzer = Lucene.Net.Analysis.Analyzer;
using CoraxAnalyzer = Corax.Analyzer;

namespace Raven.Server.Documents.Indexes.Persistence
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
        
        protected static LuceneRavenPerFieldAnalyzerWrapper CreateLuceneAnalyzer(Index index, IndexDefinitionBase indexDefinition, bool forQuerying = false)
        {
            if (indexDefinition.IndexFields.ContainsKey(Constants.Documents.Indexing.Fields.AllFields))
                throw new InvalidOperationException($"Detected '{Constants.Documents.Indexing.Fields.AllFields}'. This field should not be present here, because inheritance is done elsewhere.");

            var analyzers = new Dictionary<Type, LuceneAnalyzer>();

            var hasDefaultFieldOptions = false;
            LuceneAnalyzer defaultAnalyzerToUse = null;
            LuceneAnalyzer defaultAnalyzer = null;
            if (indexDefinition is MapIndexDefinition mid)
            {
                if (mid.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out var value))
                {
                    hasDefaultFieldOptions = true;

                    switch (value.Indexing)
                    {
                        case FieldIndexing.Exact:
                            defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer);
                            break;

                        case FieldIndexing.Search:
                            if (value.Analyzer != null)
                                defaultAnalyzerToUse = GetLuceneAnalyzer(Constants.Documents.Indexing.Fields.AllFields, value.Analyzer, analyzers, forQuerying, index.DocumentDatabase.Name);

                            if (defaultAnalyzerToUse == null)
                                defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer);
                            break;

                        default:
                            // explicitly ignore all other values
                            break;
                    }
                }
            }

            if (defaultAnalyzerToUse == null)
            {
                defaultAnalyzerToUse = defaultAnalyzer = CreateDefaultAnalyzer(Constants.Documents.Indexing.Fields.AllFields, index.Configuration.DefaultAnalyzerType.Value.Type);
                analyzers.Add(defaultAnalyzerToUse.GetType(), defaultAnalyzerToUse);
            }

            var perFieldAnalyzerWrapper = forQuerying == false && indexDefinition.HasDynamicFields
                ? new LuceneRavenPerFieldAnalyzerWrapper(
                        defaultAnalyzerToUse,
                        fieldName => GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer),
                        fieldName => GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer))
                : new LuceneRavenPerFieldAnalyzerWrapper(defaultAnalyzerToUse);

            foreach (var field in indexDefinition.IndexFields)
            {
                var fieldName = field.Value.Name;

                switch (field.Value.Indexing)
                {
                    case FieldIndexing.Exact:
                        var keywordAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer);

                        perFieldAnalyzerWrapper.AddAnalyzer(fieldName, keywordAnalyzer);
                        break;

                    case FieldIndexing.Search:
                        var analyzer = GetLuceneAnalyzer(fieldName, field.Value.Analyzer, analyzers, forQuerying, index.DocumentDatabase.Name);
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
                                defaultAnalyzer = CreateDefaultAnalyzer(fieldName, index.Configuration.DefaultAnalyzerType.Value.Type);

                            perFieldAnalyzerWrapper.AddAnalyzer(fieldName, defaultAnalyzer);
                            continue;
                        }
                        break;
                }
            }

            return perFieldAnalyzerWrapper;

            void AddStandardAnalyzer(string fieldName)
            {
                var standardAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer);

                perFieldAnalyzerWrapper.AddAnalyzer(fieldName, standardAnalyzer);
            }

            LuceneAnalyzer GetOrCreateAnalyzer(string fieldName, Type analyzerType, Func<string, Type, LuceneAnalyzer> createAnalyzer)
            {
                if (analyzers.TryGetValue(analyzerType, out var analyzer) == false)
                    analyzers[analyzerType] = analyzer = createAnalyzer(fieldName, analyzerType);

                return analyzer;
            }

            LuceneAnalyzer CreateDefaultAnalyzer(string fieldName, Type analyzerType)
            {
                if (analyzerType == typeof(LowerCaseKeywordAnalyzer))
                    return new LowerCaseKeywordAnalyzer();

                return LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
            }

            LuceneAnalyzer CreateKeywordAnalyzer(string fieldName, Type analyzerType)
            {
                if (analyzerType == typeof(KeywordAnalyzer))
                    return new KeywordAnalyzer();

                return LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
            }

            LuceneAnalyzer CreateStandardAnalyzer(string fieldName, Type analyzerType)
            {
                if (analyzerType == typeof(RavenStandardAnalyzer))
                    return new RavenStandardAnalyzer(Version.LUCENE_29);

                return LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
            }
        }

        public abstract void Dispose();

        private static LuceneAnalyzer GetLuceneAnalyzer(string fieldName, string analyzer, Dictionary<Type, LuceneAnalyzer> analyzers, bool forQuerying, string databaseName)
        {
            if (string.IsNullOrWhiteSpace(analyzer))
                return null;

            var createAnalyzer = LuceneIndexingExtensions.GetAnalyzerType(fieldName, analyzer, databaseName);

            if (forQuerying)
            {
                var notForQuerying = NotForQuerying
                    .GetOrAdd(createAnalyzer.Type, t => t.GetCustomAttributes<NotForQueryingAttribute>(false).Any());

                if (notForQuerying)
                    return null;
            }

            if (analyzers.TryGetValue(createAnalyzer.Type, out var analyzerInstance) == false)
                analyzers[createAnalyzer.Type] = analyzerInstance = createAnalyzer.CreateInstance(fieldName);

            return analyzerInstance;
        }

        private static CoraxAnalyzer GetCoraxAnalyzer(string fieldName, string analyzer, Dictionary<Type, CoraxAnalyzer> analyzers, bool forQuerying, string databaseName)
        {
            throw new NotImplementedException("Custom analyzers are not implemented now. If you need to use custom analyzer you need to use Lucene.");
        }
        
        protected static CoraxRavenPerFieldAnalyzerWrapper CreateCoraxAnalyzers(Index index, IndexDefinitionBase indexDefinition, bool forQuerying = false)
        {
            if (indexDefinition.IndexFields.ContainsKey(Constants.Documents.Indexing.Fields.AllFields))
                throw new InvalidOperationException($"Detected '{Constants.Documents.Indexing.Fields.AllFields}'. This field should not be present here, because inheritance is done elsewhere.");
        
            var analyzers = new Dictionary<Type, CoraxAnalyzer>();
            var hasDefaultFieldOptions = false;
            CoraxAnalyzer defaultAnalyzerToUse = null;
            CoraxAnalyzer defaultAnalyzer = null;
            if (indexDefinition is MapIndexDefinition mid)
            {
                if (mid.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out var value))
                {
                    hasDefaultFieldOptions = true;
        
                    switch (value.Indexing)
                    {
                        case FieldIndexing.Exact:
                            defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer);
                            break;
        
                        case FieldIndexing.Search:
                            defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer);
                            break;

                        case null:
                        case FieldIndexing.No:
                        case FieldIndexing.Default:
                        default:
                            // explicitly ignore all other values
                            break;
                    }
                }
            }
            
            if (defaultAnalyzerToUse == null)
            {
                defaultAnalyzerToUse = defaultAnalyzer = CreateDefaultAnalyzer(Constants.Documents.Indexing.Fields.AllFields, index.Configuration.DefaultAnalyzerType.Value.Type);
                analyzers.Add(defaultAnalyzerToUse.GetType(), defaultAnalyzerToUse);
            }
            
            var perFieldAnalyzerWrapper = forQuerying == false && indexDefinition.HasDynamicFields
                ? new CoraxRavenPerFieldAnalyzerWrapper(
                    defaultAnalyzerToUse,
                    fieldName => GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer),
                    fieldName => GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer), indexDefinition.IndexFields.Count + 1)
                : new CoraxRavenPerFieldAnalyzerWrapper(defaultAnalyzerToUse, indexDefinition.IndexFields.Count + 1);
            
            foreach (var field in indexDefinition.IndexFields)
            {
                var fieldName = field.Value.Name;
                var fieldId = field.Value.Id;
                
                switch (field.Value.Indexing)
                {
                    case FieldIndexing.Exact:
                        var keywordAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer);

                        perFieldAnalyzerWrapper.AddAnalyzer(fieldId, keywordAnalyzer);
                        break;

                    case FieldIndexing.Search:
                        var analyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer);
                        
                        perFieldAnalyzerWrapper.AddAnalyzer(fieldId, analyzer);
                        break;

                    case FieldIndexing.Default:
                        if (hasDefaultFieldOptions)
                        {
                            defaultAnalyzer ??= CreateDefaultAnalyzer(fieldName, index.Configuration.DefaultAnalyzerType.Value.Type);

                            perFieldAnalyzerWrapper.AddAnalyzer(fieldId, defaultAnalyzer);
                        }
                        break;
                }
            }
            
            return perFieldAnalyzerWrapper;

            CoraxAnalyzer GetOrCreateAnalyzer(string fieldName, Type analyzerType,  Func<string, Type, CoraxAnalyzer> createAnalyzer)
            {
                if (analyzers.TryGetValue(analyzerType, out var analyzer) == false)
                {
                    analyzers[analyzerType] = analyzer = createAnalyzer(fieldName, analyzerType);
                }

                return analyzer;
            }

            void AddStandardAnalyzer(string fieldName, int fieldId)
            {
                var standardAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer);

                perFieldAnalyzerWrapper.AddAnalyzer(fieldId, standardAnalyzer);
            }
            
            CoraxAnalyzer CreateDefaultAnalyzer(string fieldName, Type analyzerType)
            {
                if (analyzerType == typeof(LowerCaseKeywordAnalyzer))
                    return CoraxAnalyzer.Create(default(KeywordTokenizer), default(LowerCaseTransformer));

                if (analyzerType.IsSubclassOf(typeof(Analyzer)))
                    throw new InvalidQueryException($"Analyzer {nameof(analyzerType)} is made for Lucene. Corax doesn't support custom analyzers.");
                
                return CoraxIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
            }
            
            CoraxAnalyzer CreateKeywordAnalyzer(string fieldName, Type analyzerType)
            {
                if (analyzerType == typeof(KeywordAnalyzer))
                    return CoraxAnalyzer.Create(default(KeywordTokenizer), default(ExactTransformer));

                if (analyzerType.IsSubclassOf(typeof(Analyzer)))
                    throw new InvalidOperationException($"Analyzer {nameof(analyzerType)} is made for Lucene. Corax doesn't support custom analyzers.");

                return CoraxIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
            }
            
            CoraxAnalyzer CreateStandardAnalyzer(string fieldName, Type analyzerType)
            {
                if (analyzerType == typeof(RavenStandardAnalyzer))
                    return CoraxAnalyzer.Create(default(WhitespaceTokenizer), default(LowerCaseTransformer));

                if (analyzerType.IsSubclassOf(typeof(Analyzer)))
                    throw new InvalidOperationException($"Analyzer {nameof(analyzerType)} is made for Lucene. Corax doesn't support custom analyzers.");

                return CoraxIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
            }
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
