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
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Parser;
using Sparrow.Json;
using Sparrow.Logging;
using Query = Lucene.Net.Search.Query;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public abstract class IndexOperationBase : IDisposable
    {
        private static readonly ConcurrentDictionary<Type, bool> NotForQuerying = new ConcurrentDictionary<Type, bool>();

        protected readonly string _indexName;

        protected readonly Logger _logger;

        protected IndexOperationBase(string indexName, Logger logger)
        {
            _indexName = indexName;
            _logger = logger;
        }

        protected static RavenPerFieldAnalyzerWrapper CreateAnalyzer(Func<Analyzer> createDefaultAnalyzer, Dictionary<string, IndexField> fields, bool forQuerying = false)
        {
            if (fields.ContainsKey(Constants.Documents.Indexing.Fields.AllFields))
                throw new InvalidOperationException($"Detected '{Constants.Documents.Indexing.Fields.AllFields}'. This field should not be present here, because inheritance is done elsewhere.");

            var defaultAnalyzer = createDefaultAnalyzer();

            RavenStandardAnalyzer standardAnalyzer = null;
            KeywordAnalyzer keywordAnalyzer = null;
            var perFieldAnalyzerWrapper = new RavenPerFieldAnalyzerWrapper(defaultAnalyzer);
            foreach (var field in fields)
            {
                var fieldName = field.Key;

                switch (field.Value.Indexing)
                {
                    case FieldIndexing.NotAnalyzed:
                        if (keywordAnalyzer == null)
                            keywordAnalyzer = new KeywordAnalyzer();

                        perFieldAnalyzerWrapper.AddAnalyzer(fieldName, keywordAnalyzer);
                        break;
                    case FieldIndexing.Analyzed:
                        var analyzer = GetAnalyzer(fieldName, field.Value, forQuerying);
                        if (analyzer != null)
                        {
                            perFieldAnalyzerWrapper.AddAnalyzer(fieldName, analyzer);
                            continue;
                        }
                        AddStandardAnalyzer(fieldName);

                        break;
                }
            }

            return perFieldAnalyzerWrapper;

            void AddStandardAnalyzer(string fieldName)
            {
                if (standardAnalyzer == null)
                    standardAnalyzer = new RavenStandardAnalyzer(global::Lucene.Net.Util.Version.LUCENE_29);

                perFieldAnalyzerWrapper.AddAnalyzer(fieldName, standardAnalyzer);
            }
        }

        public abstract void Dispose();

        private static Analyzer GetAnalyzer(string name, IndexField field, bool forQuerying)
        {
            if (string.IsNullOrWhiteSpace(field.Analyzer))
                return null;

            // TODO [ppekrol] can we use one instance like with KeywordAnalyzer and StandardAnalyzer?
            var analyzerInstance = IndexingExtensions.CreateAnalyzerInstance(name, field.Analyzer);

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

        protected Query GetLuceneQuery(JsonOperationContext context, QueryMetadata metadata, BlittableJsonReaderObject parameters, Analyzer analyzer)
        {
            return GetLuceneQuery(context, metadata, metadata.Query.Where, parameters, analyzer);
        }

        protected Query GetLuceneQuery(JsonOperationContext context, QueryMetadata metadata, QueryExpression whereExpression, BlittableJsonReaderObject parameters, Analyzer analyzer)
        {
            Query documentQuery;

            if (string.IsNullOrEmpty(metadata.QueryText))
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
                    //_persistance._a
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

                    documentQuery = QueryBuilder.BuildQuery(context, metadata, whereExpression, parameters, analyzer);
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