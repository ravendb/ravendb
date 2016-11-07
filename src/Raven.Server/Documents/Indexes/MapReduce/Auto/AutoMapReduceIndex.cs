using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
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
    public class AutoMapReduceIndex : MapReduceIndexBase<AutoMapReduceIndexDefinition>
    {
        private ReduceKeyProcessor _reduceKeyProcessor;

        private readonly MapResult[] _singleOutputList = new MapResult[1]
        {
            new MapResult()
        };

        private AutoMapReduceIndex(int indexId, AutoMapReduceIndexDefinition definition)
            : base(indexId, IndexType.AutoMapReduce, definition)
        {
            ActualMaxNumberOfIndexOutputs = null;
            MaxNumberOfIndexOutputs = 1;
        }

        public static AutoMapReduceIndex CreateNew(int indexId, AutoMapReduceIndexDefinition definition,
            DocumentDatabase documentDatabase)
        {
            var instance = new AutoMapReduceIndex(indexId, definition);
            instance.Initialize(documentDatabase, documentDatabase.Configuration.Indexing);

            return instance;
        }

        public static AutoMapReduceIndex Open(int indexId, StorageEnvironment environment,
            DocumentDatabase documentDatabase)
        {
            var definition = AutoMapReduceIndexDefinition.Load(environment);
            var instance = new AutoMapReduceIndex(indexId, definition);
            instance.Initialize(environment, documentDatabase, documentDatabase.Configuration.Indexing);

            return instance;
        }

        protected override void InitializeInternal()
        {
            _reduceKeyProcessor = new ReduceKeyProcessor(Definition.GroupByFields.Count, _unmanagedBuffersPool);
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, MapReduceWorkContext),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, MapReduceWorkContext, Configuration),
                new ReduceMapResultsOfAutoIndex(this, Definition, _indexStorage, DocumentDatabase.Metrics, MapReduceWorkContext),
            };
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            return new AutoIndexDocsEnumerator(documents, stats);
        }

        public override int HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            var mappedResult = new DynamicJsonValue();

            using (stats.For(IndexingOperation.Reduce.BlittableJsonAggregation))
            {
                var document = ((Document[])mapResults)[0];
                Debug.Assert(key == document.LoweredKey);

                foreach (var indexField in Definition.MapFields.Values)
                {
                    switch (indexField.MapReduceOperation)
                    {
                        case FieldMapReduceOperation.Count:
                            mappedResult[indexField.Name] = 1;
                            break;
                        case FieldMapReduceOperation.Sum:
                            object fieldValue;
                            StringSegment leftPath;
                            BlittableJsonTraverser.Default.TryRead(document.Data, indexField.Name, out fieldValue, out leftPath);

                            var arrayResult = fieldValue as IEnumerable<object>;

                            if (arrayResult == null)
                            {
                                // explicitly adding this even if the value isn't there, as a null
                                mappedResult[indexField.Name] = fieldValue;
                                continue;
                            }

                            decimal total = 0;

                            foreach (var item in arrayResult)
                            {
                                if (item == null)
                                    continue;

                                double doubleValue;
                                long longValue;

                                switch (BlittableNumber.Parse(item, out doubleValue, out longValue))
                                {
                                    case NumberParseResult.Double:
                                        total += (decimal)doubleValue;
                                        break;
                                    case NumberParseResult.Long:
                                        total += longValue;
                                        break;
                                }
                            }

                            mappedResult[indexField.Name] = total;

                            break;
                        case FieldMapReduceOperation.None:
                            object result;
                            BlittableJsonTraverser.Default.TryRead(document.Data, indexField.Name, out result, out leftPath);

                            // explicitly adding this even if the value isn't there, as a null
                            mappedResult[indexField.Name] = result;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }

                _reduceKeyProcessor.Reset();

                foreach (var groupByFieldName in Definition.GroupByFields.Keys)
                {
                    object result;
                    StringSegment leftPath;
                    BlittableJsonTraverser.Default.TryRead(document.Data, groupByFieldName, out result, out leftPath);
                    // explicitly adding this even if the value isn't there, as a null
                    mappedResult[groupByFieldName] = result;

                    _reduceKeyProcessor.Process(indexContext.Allocator, result);
                }
            }

            BlittableJsonReaderObject mr;
            using (stats.For(IndexingOperation.Reduce.CreateBlittableJson))
                mr = indexContext.ReadObject(mappedResult, key);

            var mapResult = _singleOutputList[0];

            mapResult.Data = mr;
            mapResult.ReduceKeyHash = _reduceKeyProcessor.Hash;

            var resultsCount = PutMapResults(key, _singleOutputList, indexContext, stats);

            DocumentDatabase.Metrics.MapReduceMappedPerSecond.Mark(resultsCount);

            return resultsCount;
        }

        public override int? ActualMaxNumberOfIndexOutputs { get; }
        public override int MaxNumberOfIndexOutputs { get; }
    }
}