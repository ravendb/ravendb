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

        private readonly MapOutput _output;

        private AutoMapReduceIndex(AutoMapReduceIndexDefinition definition)
            : base(IndexType.AutoMapReduce, IndexSourceType.Documents, definition)
        {
            _isFanout = definition.GroupByFields.Any(x => x.Value.GroupByArrayBehavior == GroupByArrayBehavior.ByIndividualValues);
            _output = new MapOutput(_isFanout);
        }

        public static AutoMapReduceIndex CreateNew(AutoMapReduceIndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = new AutoMapReduceIndex(definition);
            instance.Initialize(documentDatabase, documentDatabase.Configuration.Indexing, documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        public static AutoMapReduceIndex Open(StorageEnvironment environment,
            DocumentDatabase documentDatabase)
        {
            var definition = AutoMapReduceIndexDefinition.Load(environment);
            var instance = new AutoMapReduceIndex(definition);
            instance.Initialize(environment, documentDatabase, documentDatabase.Configuration.Indexing, documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        protected override void OnInitialization()
        {
            base.OnInitialization();

            _reduceKeyProcessor = new ReduceKeyProcessor(Definition.GroupByFields.Count, _unmanagedBuffersPool);
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, MapReduceWorkContext),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, MapReduceWorkContext, Configuration),
                new ReduceMapResultsOfAutoIndex(this, Definition, _indexStorage, DocumentDatabase.Metrics, MapReduceWorkContext)
            };
        }

        public override IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexItem> items, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            return new AutoIndexDocsEnumerator(items, stats);
        }

        public override void Update(IndexDefinitionBaseServerSide definition, IndexingConfiguration configuration)
        {
            SetPriority(definition.Priority);
        }

        public override void SetState(IndexState state, bool inMemoryOnly = false, bool ignoreWriteError = false)
        {
            base.SetState(state, inMemoryOnly, ignoreWriteError);
            Definition.State = state;
        }

        public override (ICollection<string> Static, ICollection<string> Dynamic) GetEntriesFields()
        {
            var staticEntries = Definition
                .IndexFields
                .Keys
                .ToHashSet();

            var dynamicEntries = GetDynamicEntriesFields(staticEntries);

            return (staticEntries, dynamicEntries);
        }

        protected override void LoadValues()
        {
            base.LoadValues();
            Definition.State = State;
        }

        public override int HandleMap(IndexItem indexItem, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            var document = ((Document[])mapResults)[0];
            Debug.Assert(indexItem.LowerId == document.LowerId);

            using (_stats.BlittableJsonAggregation.Start())
            {
                DynamicJsonValue singleResult = null;

                var groupByFieldsCount = Definition.OrderedGroupByFields.Length;

                for (var i = 0; i < groupByFieldsCount; i++)
                {
                    var groupByField = Definition.OrderedGroupByFields[i];

                    BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, document, groupByField.Name, out object result);

                    if (_isFanout == false)
                    {
                        if (singleResult == null)
                            singleResult = new DynamicJsonValue();

                        singleResult[groupByField.Name] = result;
                        _reduceKeyProcessor.Process(indexContext.Allocator, result);
                    }
                    else
                    {
                        if (result is IEnumerable array && groupByField.GroupByArrayBehavior == GroupByArrayBehavior.ByIndividualValues)
                        {
                            // fanout
                            foreach (var item in array)
                            {
                                _output.AddGroupByValue(groupByField.Name, item);
                            }
                        }
                        else
                        {
                            _output.AddGroupByValue(groupByField.Name, result);
                        }
                    }
                }

                if (_isFanout == false)
                {
                    _output.Results.Add((singleResult, _reduceKeyProcessor.Hash));
                }
                else if (_output.GroupByFields.Count >= Definition.GroupByFields.Count)
                {
                    for (var i = 0; i < _output.MaxGroupByFieldsCount; i++)
                    {
                        var json = new DynamicJsonValue();

                        foreach (var groupBy in _output.GroupByFields)
                        {
                            var index = Math.Min(i, groupBy.Value.Count - 1);
                            var value = _output.GroupByFields[groupBy.Key][index];

                            json[groupBy.Key] = value;

                            _reduceKeyProcessor.Process(indexContext.Allocator, value);
                        }

                        _output.Results.Add((json, _reduceKeyProcessor.Hash));

                        _reduceKeyProcessor.Reset();
                    }
                }
                // else { } - we have fanout index with multiple group by fields and one is collection
                // if we have empty collection we cannot create composite key then
                // let's skip putting such map results

                foreach (var field in Definition.MapFields)
                {
                    var autoIndexField = field.Value.As<AutoIndexField>();

                    var value = GetFieldValue(autoIndexField, document);

                    if (_isFanout == false)
                        _output.Results[0].Json[autoIndexField.Name] = value;
                    else
                    {
                        var fanoutIndex = 0;

                        if (!(value is IEnumerable array))
                        {
                            for (; fanoutIndex < _output.Results.Count; fanoutIndex++)
                            {
                                _output.Results[fanoutIndex].Json[autoIndexField.Name] = value;
                            }
                        }
                        else
                        {
                            foreach (var arrayValue in array)
                            {
                                Debug.Assert(fanoutIndex < _output.Results.Count);
                                _output.Results[fanoutIndex++].Json[autoIndexField.Name] = arrayValue;
                            }
                        }
                    }
                }
            }

            try
            {
                using (_stats.CreateBlittableJson.Start())
                {
                    for (var i = 0; i < _output.Results.Count; i++)
                    {
                        _results.Add(new MapResult
                        {
                            Data = indexContext.ReadObject(_output.Results[i].Json, "map result"),
                            ReduceKeyHash = _output.Results[i].ReduceKeyHash
                        });
                    }
                }

                var resultsCount = PutMapResults(indexItem.LowerId, indexItem.Id, _results, indexContext, stats);

                DocumentDatabase.Metrics.MapReduceIndexes.MappedPerSec.Mark(resultsCount);

                return resultsCount;
            }
            finally
            {
                _results.Clear();
                _reduceKeyProcessor.Reset();
                _output.Reset();
            }
        }

        private object GetFieldValue(AutoIndexField autoIndexField, Document document)
        {
            switch (autoIndexField.Aggregation)
            {
                case AggregationOperation.Count:
                    return 1;
                case AggregationOperation.Sum:
                    BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, document, autoIndexField.Name, out object fieldValue);

                    var arrayResult = fieldValue as IEnumerable<object>;

                    if (arrayResult == null)
                    {
                        // explicitly adding this even if the value isn't there, as a null
                        return fieldValue;
                    }


                    if (_isFanout == false)
                    {
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

                        return total;
                    }

                    // if fanout then we need to insert each value separately in map results
                    return arrayResult;
                case AggregationOperation.None:
                    BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, document, autoIndexField.Name, out object result);

                    // explicitly adding this even if the value isn't there, as a null
                    return result;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        protected override void DisposeIndex()
        {
            base.DisposeIndex();
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
            public MapOutput(bool isFanout)
            {
                Results = new List<(DynamicJsonValue Json, ulong ReduceKeyHash)>(isFanout == false ? 1 : 4);

                if (isFanout)
                    GroupByFields = new Dictionary<string, List<object>>();
            }

            public readonly List<(DynamicJsonValue Json, ulong ReduceKeyHash)> Results;

            public readonly Dictionary<string, List<object>> GroupByFields;

            public int MaxGroupByFieldsCount;

            public void AddGroupByValue(string field, object value)
            {
                if (GroupByFields.TryGetValue(field, out var values) == false)
                {
                    values = new List<object>();
                    GroupByFields.Add(field, values);
                }

                values.Add(value);

                MaxGroupByFieldsCount = Math.Max(values.Count, MaxGroupByFieldsCount);
            }

            public void Reset()
            {
                Results.Clear();
                GroupByFields?.Clear();
                MaxGroupByFieldsCount = 0;
            }
        }
    }
}
