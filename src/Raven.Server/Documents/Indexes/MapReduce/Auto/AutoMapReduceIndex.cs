using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce.Auto
{
    public class AutoMapReduceIndex : MapReduceIndexBase<AutoMapReduceIndexDefinition, AutoIndexField>
    {
        private ReduceKeyProcessor _reduceKeyProcessor;

        private IndexingStatsScope _statsInstance;
        private readonly MapPhaseStats _stats = new MapPhaseStats();

        private readonly MapResult[] _singleOutputList = {
            new MapResult()
        };

        private AutoMapReduceIndex(long etag, AutoMapReduceIndexDefinition definition)
            : base(etag, IndexType.AutoMapReduce, definition)
        {
        }

        public static AutoMapReduceIndex CreateNew(long etag, AutoMapReduceIndexDefinition definition,
            DocumentDatabase documentDatabase)
        {
            var instance = new AutoMapReduceIndex(etag, definition);
            instance.Initialize(documentDatabase, documentDatabase.Configuration.Indexing, documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        public static AutoMapReduceIndex Open(long etag, StorageEnvironment environment,
            DocumentDatabase documentDatabase)
        {
            var definition = AutoMapReduceIndexDefinition.Load(environment);
            var instance = new AutoMapReduceIndex(etag, definition);
            instance.Initialize(environment, documentDatabase, documentDatabase.Configuration.Indexing, documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        protected override void InitializeInternal()
        {
            base.InitializeInternal();

            _reduceKeyProcessor = new ReduceKeyProcessor(Definition.GroupByFields.Count, _unmanagedBuffersPool);
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, MapReduceWorkContext),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, MapReduceWorkContext, Configuration),
                new ReduceMapResultsOfAutoIndex(this, Definition, _indexStorage, DocumentDatabase.Metrics, MapReduceWorkContext)
            };
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            return new AutoIndexDocsEnumerator(documents, stats);
        }

        public override void Update(IndexDefinitionBase definition, IndexingConfiguration configuration)
        {
            SetPriority(definition.Priority);
        }

        public override int HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            var mappedResult = new DynamicJsonValue();

            using (_stats.BlittableJsonAggregation.Start())
            {
                var document = ((Document[])mapResults)[0];
                Debug.Assert(key == document.LowerId);

                foreach (var field in Definition.MapFields.Values)
                {
                    var autoIndexField = field.As<AutoIndexField>();

                    switch (autoIndexField.Aggregation)
                    {
                        case AggregationOperation.Count:
                            mappedResult[autoIndexField.Name] = 1;
                            break;
                        case AggregationOperation.Sum:
                            object fieldValue;
                            BlittableJsonTraverser.Default.TryRead(document.Data, autoIndexField.Name, out fieldValue, out _);

                            var arrayResult = fieldValue as IEnumerable<object>;

                            if (arrayResult == null)
                            {
                                // explicitly adding this even if the value isn't there, as a null
                                mappedResult[autoIndexField.Name] = fieldValue;
                                continue;
                            }

                            decimal total = 0;

                            foreach (var item in arrayResult)
                            {
                                if (item == null)
                                    continue;

                                switch (BlittableNumber.Parse(item, out double doubleValue, out long longValue))
                                {
                                    case NumberParseResult.Double:
                                        total += (decimal)doubleValue;
                                        break;
                                    case NumberParseResult.Long:
                                        total += longValue;
                                        break;
                                }
                            }

                            mappedResult[autoIndexField.Name] = total;

                            break;
                        case AggregationOperation.None:
                            object result;
                            BlittableJsonTraverser.Default.TryRead(document.Data, autoIndexField.Name, out result, out _);

                            // explicitly adding this even if the value isn't there, as a null
                            mappedResult[autoIndexField.Name] = result;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }

                _reduceKeyProcessor.Reset();

                foreach (var groupByField in Definition.GroupByFields)
                {
                    BlittableJsonTraverser.Default.TryRead(document.Data, groupByField.Key, out object result, out StringSegment _);
                    // explicitly adding this even if the value isn't there, as a null
                    mappedResult[groupByField.Key] = result;

                    _reduceKeyProcessor.Process(indexContext.Allocator, result);
                }
            }

            BlittableJsonReaderObject mr;
            using (_stats.CreateBlittableJson.Start())
                mr = indexContext.ReadObject(mappedResult, key);

            var mapResult = _singleOutputList[0];

            mapResult.Data = mr;
            mapResult.ReduceKeyHash = _reduceKeyProcessor.Hash;

            var resultsCount = PutMapResults(key, _singleOutputList, indexContext, stats);

            DocumentDatabase.Metrics.MapReduceMappedPerSecond.Mark(resultsCount);

            return resultsCount;
        }

        public override void Dispose()
        {
            base.Dispose();
            _reduceKeyProcessor.ReleaseBuffer();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureValidStats(IndexingStatsScope stats)
        {
            if (_statsInstance == stats)
                return;

            _statsInstance = stats;

            _stats.BlittableJsonAggregation = stats.For(IndexingOperation.Reduce.BlittableJsonAggregation, start: false);
            _stats.CreateBlittableJson = stats.For(IndexingOperation.Reduce.CreateBlittableJson, start: false);
        }

        private class MapPhaseStats
        {
            public IndexingStatsScope BlittableJsonAggregation;
            public IndexingStatsScope CreateBlittableJson;
        }
    }
}
