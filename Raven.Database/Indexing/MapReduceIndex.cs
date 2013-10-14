//-----------------------------------------------------------------------
// <copyright file="MapReduceIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database.Data;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
	public class MapReduceIndex : Index
	{
		readonly JsonSerializer jsonSerializer;

		private class IgnoreFieldable : JsonConverter
		{
			public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
			{
				writer.WriteValue("IgnoredLuceueField");
			}

			public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
			{
				return null;
			}

			public override bool CanConvert(Type objectType)
			{
				return typeof(IFieldable).IsAssignableFrom(objectType) ||
					   typeof(IEnumerable<AbstractField>).IsAssignableFrom(objectType);
			}
		}

        public MapReduceIndex(Directory directory, int id, IndexDefinition indexDefinition,
							  AbstractViewGenerator viewGenerator, WorkContext context)
            : base(directory, id, indexDefinition, viewGenerator, context)
		{
			jsonSerializer = new JsonSerializer();
			foreach (var jsonConverter in Default.Converters)
			{
				jsonSerializer.Converters.Add(jsonConverter);
			}
			jsonSerializer.Converters.Add(new IgnoreFieldable());
		}

		public override bool IsMapReduce
		{
			get { return true; }
		}

		public override void IndexDocuments(
			AbstractViewGenerator viewGenerator,
			IndexingBatch batch,
			IStorageActionsAccessor actions,
			DateTime minimumTimestamp)
		{
			var count = 0;
			var sourceCount = 0;
			var sw = Stopwatch.StartNew();
			var start = SystemTime.UtcNow;
			var deleted = new Dictionary<ReduceKeyAndBucket, int>();
			RecordCurrentBatch("Current Map", batch.Docs.Count);
			var documentsWrapped = batch.Docs.Select(doc =>
			{
				sourceCount++;
				var documentId = doc.__document_id;
				actions.MapReduce.DeleteMappedResultsForDocumentId((string)documentId, indexId, deleted);
				return doc;
			})
				.Where(x => x is FilteredDocument == false)
				.ToList();
			var allReferencedDocs = new ConcurrentQueue<IDictionary<string, HashSet<string>>>();
            var missingReferencedDocs = new ConcurrentQueue<HashSet<string>>();
			var allState = new ConcurrentQueue<Tuple<HashSet<ReduceKeyAndBucket>, IndexingWorkStats, Dictionary<string, int>>>();
			BackgroundTaskExecuter.Instance.ExecuteAllBuffered(context, documentsWrapped, partition =>
			{
				var localStats = new IndexingWorkStats();
				var localChanges = new HashSet<ReduceKeyAndBucket>();
				var statsPerKey = new Dictionary<string, int>();
				allState.Enqueue(Tuple.Create(localChanges, localStats, statsPerKey));

                using (CurrentIndexingScope.Current = new CurrentIndexingScope(LoadDocument, (references,
                                                                                                     missing) =>
				{
				    allReferencedDocs.Enqueue(references);
                    missingReferencedDocs.Enqueue(missing);
				}))
				{
					// we are writing to the transactional store from multiple threads here, and in a streaming fashion
					// should result in less memory and better perf
					context.TransactionalStorage.Batch(accessor =>
					{
						var mapResults = RobustEnumerationIndex(partition, viewGenerator.MapDefinitions, localStats);
						var currentDocumentResults = new List<object>();
						string currentKey = null;
						foreach (var currentDoc in mapResults)
						{
							var documentId = GetDocumentId(currentDoc);
							if (documentId != currentKey)
							{
								count += ProcessBatch(viewGenerator, currentDocumentResults, currentKey, localChanges, accessor, statsPerKey);
								currentDocumentResults.Clear();
								currentKey = documentId;
							}
							currentDocumentResults.Add(new DynamicJsonObject(RavenJObject.FromObject(currentDoc, jsonSerializer)));

							EnsureValidNumberOfOutputsForDocument(documentId, currentDocumentResults.Count);

							Interlocked.Increment(ref localStats.IndexingSuccesses);
						}
						count += ProcessBatch(viewGenerator, currentDocumentResults, currentKey, localChanges, accessor, statsPerKey);
					});
				}
			});


			
			UpdateDocumentReferences(actions, allReferencedDocs, missingReferencedDocs);

			var changed = allState.SelectMany(x => x.Item1).Concat(deleted.Keys)
					.Distinct()
					.ToList();

			var stats = new IndexingWorkStats(allState.Select(x => x.Item2));
			var reduceKeyStats = allState.SelectMany(x => x.Item3)
										 .GroupBy(x => x.Key)
										 .Select(g => new { g.Key, Count = g.Sum(x => x.Value) })
										 .ToList();

			BackgroundTaskExecuter.Instance.ExecuteAllBuffered(context, reduceKeyStats, enumerator => context.TransactionalStorage.Batch(accessor =>
			{
				while (enumerator.MoveNext())
				{
					var reduceKeyStat = enumerator.Current;
                    accessor.MapReduce.IncrementReduceKeyCounter(indexId, reduceKeyStat.Key, reduceKeyStat.Count);
				}
			}));

			BackgroundTaskExecuter.Instance.ExecuteAllBuffered(context, changed, enumerator => context.TransactionalStorage.Batch(accessor =>
			{
				while (enumerator.MoveNext())
				{
                    accessor.MapReduce.ScheduleReductions(indexId, 0, enumerator.Current);
				}
			}));


			UpdateIndexingStats(context, stats);
			AddindexingPerformanceStat(new IndexingPerformanceStats
			{
				OutputCount = count,
				ItemsCount = sourceCount,
				InputCount = documentsWrapped.Count,
				Operation = "Map",
				Duration = sw.Elapsed,
				Started = start
			});
			BatchCompleted("Current Map");
            logIndexing.Debug("Mapped {0} documents for {1}", count, indexId);
		}

		private int ProcessBatch(AbstractViewGenerator viewGenerator, List<object> currentDocumentResults, string currentKey, HashSet<ReduceKeyAndBucket> changes,
			IStorageActionsAccessor actions,
			IDictionary<string, int> statsPerKey)
		{
			if (currentKey == null || currentDocumentResults.Count == 0)
				return 0;


			int count = 0;
			var results = RobustEnumerationReduceDuringMapPhase(currentDocumentResults.GetEnumerator(), viewGenerator.ReduceDefinition);
			foreach (var doc in results)
			{
				count++;

				var reduceValue = viewGenerator.GroupByExtraction(doc);
				if (reduceValue == null)
				{
					logIndexing.Debug("Field {0} is used as the reduce key and cannot be null, skipping document {1}",
									  viewGenerator.GroupByExtraction, currentKey);
					continue;
				}
				string reduceKey = ReduceKeyToString(reduceValue);

				var data = GetMappedData(doc);

                actions.MapReduce.PutMappedResult(indexId, currentKey, reduceKey, data);
				statsPerKey[reduceKey] = statsPerKey.GetOrDefault(reduceKey) + 1;
				actions.General.MaybePulseTransaction();
				changes.Add(new ReduceKeyAndBucket(IndexingUtil.MapBucket(currentKey), reduceKey));
			}
			return count;
		}

		private RavenJObject GetMappedData(object doc)
		{
			if (doc is IDynamicJsonObject)
				return ((IDynamicJsonObject)doc).Inner;

			var ravenJTokenWriter = new RavenJTokenWriter();
			jsonSerializer.Serialize(ravenJTokenWriter, doc);
			return (RavenJObject)ravenJTokenWriter.Token;
		}

		private static readonly ConcurrentDictionary<Type, Func<object, object>> documentIdFetcherCache =
			new ConcurrentDictionary<Type, Func<object, object>>();

		private static string GetDocumentId(object doc)
		{
			var docIdFetcher = documentIdFetcherCache.GetOrAdd(doc.GetType(), type =>
			{
				// document may be DynamicJsonObject if we are using compiled views
				if (typeof(DynamicJsonObject) == type)
				{
					return i => ((dynamic)i).__document_id;
				}
				var docIdProp = TypeDescriptor.GetProperties(doc).Find(Constants.DocumentIdFieldName, false);
				return docIdProp.GetValue;
			});
			if (docIdFetcher == null)
				throw new InvalidOperationException("Could not create document id fetcher for this document");
			var documentId = docIdFetcher(doc);
			if (documentId == null || documentId is DynamicNullObject)
				throw new InvalidOperationException("Could not getdocument id fetcher for this document");

			return (string)documentId;
		}

		internal static string ReduceKeyToString(object reduceValue)
		{
			if (reduceValue is string)
			{
				return reduceValue.ToString();
			}
			if (reduceValue is DateTime)
                return ((DateTime)reduceValue).ToString(Default.DateTimeFormatsToWrite);
			if (reduceValue is DateTimeOffset)
				return ((DateTimeOffset)reduceValue).ToString(Default.DateTimeFormatsToWrite, CultureInfo.InvariantCulture);
			if (reduceValue is ValueType)
				return reduceValue.ToString();

			var dynamicJsonObject = reduceValue as IDynamicJsonObject;
			if (dynamicJsonObject != null)
				return dynamicJsonObject.Inner.ToString(Formatting.None);
			return RavenJToken.FromObject(reduceValue).ToString(Formatting.None);
		}

		protected override IndexQueryResult RetrieveDocument(Document document, FieldsToFetch fieldsToFetch, ScoreDoc score)
		{
			fieldsToFetch.EnsureHasField(Constants.ReduceKeyFieldName);
			if (fieldsToFetch.IsProjection)
			{
				return base.RetrieveDocument(document, fieldsToFetch, score);
			}
			var field = document.GetField(Constants.ReduceValueFieldName);
			if (field == null)
			{
				fieldsToFetch = fieldsToFetch.CloneWith(document.GetFields().Select(x => x.Name).ToArray());
				return base.RetrieveDocument(document, fieldsToFetch, score);
			}
			return new IndexQueryResult
			{
				Projection = RavenJObject.Parse(field.StringValue),
				Score = score.Score
			};
		}

		protected override void HandleCommitPoints(IndexedItemsInfo itemsInfo)
		{
			// MapReduce index does not store and use any commit points
		}

		protected override bool IsUpToDateEnoughToWriteToDisk(Etag highestETag)
		{
			// for map/reduce indexes, we always write to disk, the in memory optimization
			// isn't really doing much for us, since we already write the intermediate results 
			// to disk anyway, so it doesn't matter
			return true;
		}

		public override void Remove(string[] keys, WorkContext context)
		{
			context.TransactionalStorage.Batch(actions =>
			{
				var reduceKeyAndBuckets = new Dictionary<ReduceKeyAndBucket, int>();
				foreach (var key in keys)
				{
                    actions.MapReduce.DeleteMappedResultsForDocumentId(key, indexId, reduceKeyAndBuckets);
				}

                actions.MapReduce.UpdateRemovedMapReduceStats(indexId, reduceKeyAndBuckets);
				foreach (var reduceKeyAndBucket in reduceKeyAndBuckets)
				{
                    actions.MapReduce.ScheduleReductions(indexId, 0, reduceKeyAndBucket.Key);
				}
			});
		}

		public class ReduceDocuments
		{
			private readonly MapReduceIndex parent;
			private readonly int inputCount;
            private readonly int indexId;
			readonly AnonymousObjectToLuceneDocumentConverter anonymousObjectToLuceneDocumentConverter;
			private readonly Document luceneDoc = new Document();
			private readonly Field reduceValueField = new Field(Constants.ReduceValueFieldName, "dummy",
													 Field.Store.YES, Field.Index.NO);

			private readonly Field reduceKeyField = new Field(Constants.ReduceKeyFieldName, "dummy",
													 Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
			private PropertyDescriptorCollection properties = null;
			private readonly List<AbstractIndexUpdateTriggerBatcher> batchers;

			public ReduceDocuments(MapReduceIndex parent, AbstractViewGenerator viewGenerator, IEnumerable<IGrouping<int, object>> mappedResultsByBucket, int level, WorkContext context, IStorageActionsAccessor actions, HashSet<string> reduceKeys, int inputCount)
			{
				this.parent = parent;
				this.inputCount = inputCount;
                indexId = this.parent.indexId;
				ViewGenerator = viewGenerator;
				MappedResultsByBucket = mappedResultsByBucket;
				Level = level;
				Context = context;
				Actions = actions;
				ReduceKeys = reduceKeys;

                anonymousObjectToLuceneDocumentConverter = new AnonymousObjectToLuceneDocumentConverter(this.parent.context.Database, this.parent.indexDefinition, ViewGenerator);

				if (Level == 2)
				{
                    batchers = Context.IndexUpdateTriggers.Select(x => x.CreateBatcher(indexId))
								.Where(x => x != null)
								.ToList();
				}
			}

			public AbstractViewGenerator ViewGenerator { get; private set; }
			public IEnumerable<IGrouping<int, object>> MappedResultsByBucket { get; private set; }
			public int Level { get; private set; }
			public WorkContext Context { get; private set; }
			public IStorageActionsAccessor Actions { get; private set; }
			public HashSet<string> ReduceKeys { get; private set; }

			private string ExtractReduceKey(AbstractViewGenerator viewGenerator, object doc)
			{
				try
				{
					object reduceKey = viewGenerator.GroupByExtraction(doc);
					if (reduceKey == null)
					{
                        throw new InvalidOperationException("Could not find reduce key for " + indexId + " in the result: " + doc);
					}
					return ReduceKeyToString(reduceKey);
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("Could not extract reduce key from reduce result!", e);
				}
			}

			private IEnumerable<AbstractField> GetFields(object doc, out float boost)
			{
				boost = 1;
				var boostedValue = doc as BoostedValue;
				if (boostedValue != null)
				{
					doc = boostedValue.Value;
					boost = boostedValue.Boost;
				}
				IEnumerable<AbstractField> fields;
				if (doc is IDynamicJsonObject)
				{
					fields = anonymousObjectToLuceneDocumentConverter.Index(((IDynamicJsonObject)doc).Inner, Field.Store.NO);
				}
				else
				{
					properties = properties ?? TypeDescriptor.GetProperties(doc);
					fields = anonymousObjectToLuceneDocumentConverter.Index(doc, properties, Field.Store.NO);
				}
				if (Math.Abs(boost - 1) > float.Epsilon)
				{
					var abstractFields = fields.ToList();
					foreach (var abstractField in abstractFields)
					{
						abstractField.OmitNorms = false;
					}
					return abstractFields;
				}
				return fields;
			}

			private static RavenJObject ToJsonDocument(object doc)
			{
				var boostedValue = doc as BoostedValue;
				if (boostedValue != null)
				{
					doc = boostedValue.Value;
				}
				var dynamicJsonObject = doc as IDynamicJsonObject;
				if (dynamicJsonObject != null)
				{
					return dynamicJsonObject.Inner;
				}
				var ravenJObject = doc as RavenJObject;
				if (ravenJObject != null)
					return ravenJObject;
				var jsonDocument = RavenJObject.FromObject(doc);
				MergeArrays(jsonDocument);

				// remove _, __, etc fields
				foreach (var prop in jsonDocument.Where(x => x.Key.All(ch => ch == '_')).ToArray())
				{
					jsonDocument.Remove(prop.Key);
				}
				return jsonDocument;
			}

			private static void MergeArrays(RavenJToken token)
			{
				if (token == null)
					return;
				switch (token.Type)
				{
					case JTokenType.Array:
						var arr = (RavenJArray)token;
						for (int i = 0; i < arr.Length; i++)
						{
							var current = arr[i];
							if (current == null || current.Type != JTokenType.Array)
								continue;
							arr.RemoveAt(i);
							i--;
							var j = Math.Max(0, i);
							foreach (var item in (RavenJArray)current)
							{
								arr.Insert(j++, item);
							}
						}
						break;
					case JTokenType.Object:
						foreach (var kvp in ((RavenJObject)token))
						{
							MergeArrays(kvp.Value);
						}
						break;
				}
			}

			public void ExecuteReduction()
			{
				var count = 0;
				var sourceCount = 0;
				var sw = Stopwatch.StartNew();
				var start = SystemTime.UtcNow;

				parent.Write((indexWriter, analyzer, stats) =>
				{
					stats.Operation = IndexingWorkStats.Status.Reduce;
					try
					{
						parent.RecordCurrentBatch("Current Reduce #" + Level, MappedResultsByBucket.Sum(x => x.Count()));
						if (Level == 2)
						{
							RemoveExistingReduceKeysFromIndex(indexWriter);
						}
						foreach (var mappedResults in MappedResultsByBucket)
						{
							var input = mappedResults.Select(x =>
							{
								sourceCount++;
								return x;
							});
							foreach (var doc in parent.RobustEnumerationReduce(input.GetEnumerator(), ViewGenerator.ReduceDefinition, Actions, stats))
							{
								count++;
								string reduceKeyAsString = ExtractReduceKey(ViewGenerator, doc);

								switch (Level)
								{
									case 0:
									case 1:
                                        Actions.MapReduce.PutReducedResult(indexId, reduceKeyAsString, Level + 1, mappedResults.Key, mappedResults.Key / 1024, ToJsonDocument(doc));
										Actions.General.MaybePulseTransaction();
										break;
									case 2:
										WriteDocumentToIndex(doc, indexWriter, analyzer);
										break;
									default:
										throw new InvalidOperationException("Unknown level: " + Level);
								}
								stats.ReduceSuccesses++;
							}
						}
					}
					catch (Exception e)
					{
						if (Level == 2)
						{
							batchers.ApplyAndIgnoreAllErrors(
								ex =>
								{
									logIndexing.WarnException("Failed to notify index update trigger batcher about an error", ex);
                                    Context.AddError(indexId, parent.indexDefinition.Name, null, ex.Message, "AnErrorOccured Trigger");
								},
								x => x.AnErrorOccured(e));
						}
						throw;
					}
					finally
					{
						if (Level == 2)
						{
							batchers.ApplyAndIgnoreAllErrors(
								e =>
								{
									logIndexing.WarnException("Failed to dispose on index update trigger", e);
                                    Context.AddError(indexId, parent.indexDefinition.Name, null, e.Message, "Dispose Trigger");
								},
								x => x.Dispose());
						}
						parent.BatchCompleted("Current Reduce #" + Level);
					}
					return new IndexedItemsInfo
					{
						ChangedDocs = count + ReduceKeys.Count
					};
				});
				parent.AddindexingPerformanceStat(new IndexingPerformanceStats
				{
					OutputCount = count,
					ItemsCount = sourceCount,
					InputCount = inputCount,
					Duration = sw.Elapsed,
					Operation = "Reduce Level " + Level,
					Started = start
				});
                logIndexing.Debug(() => string.Format("Reduce resulted in {0} entries for {1} for reduce keys: {2}", count, indexId, string.Join(", ", ReduceKeys)));
			}

			private void WriteDocumentToIndex(object doc, RavenIndexWriter indexWriter, Analyzer analyzer)
			{
				float boost;
				var fields = GetFields(doc, out boost).ToList();

				string reduceKeyAsString = ExtractReduceKey(ViewGenerator, doc);
				reduceKeyField.SetValue(reduceKeyAsString);
				reduceValueField.SetValue(ToJsonDocument(doc).ToString(Formatting.None));
				luceneDoc.GetFields().Clear();
				luceneDoc.Boost = boost;
				luceneDoc.Add(reduceKeyField);
				luceneDoc.Add(reduceValueField);
				foreach (var field in fields)
				{
					luceneDoc.Add(field);
				}

				batchers.ApplyAndIgnoreAllErrors(
					exception =>
					{
						logIndexing.WarnException(
							string.Format("Error when executed OnIndexEntryCreated trigger for index '{0}', key: '{1}'",
                                          indexId, reduceKeyAsString),
							exception);
                        Context.AddError(indexId, parent.PublicName, reduceKeyAsString, exception.Message, "OnIndexEntryCreated Trigger");
					},
					trigger => trigger.OnIndexEntryCreated(reduceKeyAsString, luceneDoc));

				parent.LogIndexedDocument(reduceKeyAsString, luceneDoc);

				parent.AddDocumentToIndex(indexWriter, luceneDoc, analyzer);
			}

			private void RemoveExistingReduceKeysFromIndex(RavenIndexWriter indexWriter)
			{
				foreach (var reduceKey in ReduceKeys)
				{
					var entryKey = reduceKey;
					indexWriter.DeleteDocuments(new Term(Constants.ReduceKeyFieldName, entryKey));
					batchers.ApplyAndIgnoreAllErrors(
						exception =>
						{
							logIndexing.WarnException(
								string.Format("Error when executed OnIndexEntryDeleted trigger for index '{0}', key: '{1}'",
                                              indexId, entryKey),
								exception);
                            Context.AddError(indexId, parent.PublicName, entryKey, exception.Message, "OnIndexEntryDeleted Trigger");
						},
						trigger => trigger.OnIndexEntryDeleted(entryKey));
				}
			}
		}
	}
}
