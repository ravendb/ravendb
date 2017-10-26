using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
        private readonly bool _isFanout;

        private readonly List<MapResult> _results = new List<MapResult>();

        private AutoMapReduceIndex(long etag, AutoMapReduceIndexDefinition definition)
            : base(etag, IndexType.AutoMapReduce, definition)
        {
            _isFanout = definition.GroupByFields.Any(x => x.Value.GroupByArrayBehavior == GroupByArrayBehavior.ByIndividualValues);
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

        public override int HandleMap(LazyStringValue lowerId, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            var output = new MapOutput();

            using (_stats.BlittableJsonAggregation.Start())
            {
                var document = ((Document[])mapResults)[0];
                Debug.Assert(lowerId == document.LowerId);

                foreach (var field in Definition.MapFields.Values)
                {
                    var autoIndexField = field.As<AutoIndexField>();

                    switch (autoIndexField.Aggregation)
                    {
                        case AggregationOperation.Count:
                            output.Json[autoIndexField.Name] = 1;
                            break;
                        case AggregationOperation.Sum:
                            BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, document, autoIndexField.Name, out object fieldValue);

                            var arrayResult = fieldValue as IEnumerable<object>;

                            if (arrayResult == null)
                            {
                                // explicitly adding this even if the value isn't there, as a null
                                output.Json[autoIndexField.Name] = fieldValue;
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

                            output.Json[autoIndexField.Name] = total;
                            break;
                        case AggregationOperation.None:
                            BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, document, autoIndexField.Name, out object result);

                            // explicitly adding this even if the value isn't there, as a null
                            output.Json[autoIndexField.Name] = result;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }

                _reduceKeyProcessor.Reset();

                foreach (var groupByField in Definition.GroupByFields)
                {
                    BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, document, groupByField.Key, out object result);

                    if (_isFanout == false)
                    {
                        // explicitly adding this even if the value isn't there, as a null
                        output.Json[groupByField.Key] = result;

                        _reduceKeyProcessor.Process(indexContext.Allocator, result);
                    }
                    else
                    {
                        if (result is IEnumerable array)
                        {
                            switch (groupByField.Value.GroupByArrayBehavior)
                            {
                                case GroupByArrayBehavior.ByContent:
                                    // just put entire array as a value
                                    break;
                                case GroupByArrayBehavior.ByIndividualValues:

                                    foreach (var item in array)
                                    {
                                        output.AddGroupByValue(groupByField.Key, item);
                                    }

                                    continue;
                                case GroupByArrayBehavior.NotApplicable:
                                    ThrowUndefinedGroupByArrayBehavior(groupByField.Value.Name);
                                    break;
                            }
                        }

                        output.AddGroupByValue(groupByField.Key, result);
                    }
                }
            }

            try
            {
                using (_stats.CreateBlittableJson.Start())
                {
                    if (_isFanout == false)
                    {
                        _results.Add(new MapResult
                        {
                            Data = indexContext.ReadObject(output.Json, "map result"),
                            ReduceKeyHash = _reduceKeyProcessor.Hash
                        });
                    }
                    else
                    {
                        for (int i = 0; i < output.MaxGroupByValuesCount; i++)
                        {
                            _reduceKeyProcessor.Reset();

                            var json = new DynamicJsonValue();

                            foreach (var property in output.Json.Properties)
                            {
                                json[property.Name] = property.Value;
                            }

                            foreach (var groupBy in output.GroupByFields)
                            {
                                var index = Math.Min(i, groupBy.Value.Count - 1);
                                var value = output.GroupByFields[groupBy.Key][index];

                                json[groupBy.Key] = value;

                                _reduceKeyProcessor.Process(indexContext.Allocator, value);
                            }

                            _results.Add(new MapResult
                            {
                                Data = indexContext.ReadObject(json, "map result"),
                                ReduceKeyHash = _reduceKeyProcessor.Hash
                            });
                        }
                    }
                }

                var resultsCount = PutMapResults(lowerId, _results, indexContext, stats);
                
                DocumentDatabase.Metrics.MapReduceIndexes.MappedPerSec.Mark(resultsCount);

                return resultsCount;
            }
            finally
            {
                _results.Clear();
            }
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

        private class MapOutput
        {
            public MapOutput()
            {
                Json = new DynamicJsonValue();
            }

            public readonly DynamicJsonValue Json;

            public Dictionary<string, List<object>> GroupByFields;

            public int MaxGroupByValuesCount;

            public void AddGroupByValue(string field, object value)
            {
                if (GroupByFields == null)
                    GroupByFields = new Dictionary<string, List<object>>();

                if (GroupByFields.TryGetValue(field, out var values) == false)
                {
                    values = new List<object>();
                    GroupByFields.Add(field, values);
                }

                values.Add(value);

                MaxGroupByValuesCount = Math.Max(values.Count, MaxGroupByValuesCount);
            }
        }

        private static void ThrowUndefinedGroupByArrayBehavior(string fieldName)
        {
            throw new InvalidOperationException($"There is no behavior defined for grouping by array. Field name: {fieldName}");
        }
    }
}
