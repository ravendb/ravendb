//-----------------------------------------------------------------------
// <copyright file="Index.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Search.Vectorhighlight;
using Lucene.Net.Store;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Database.Util;
using Raven.Json.Linq;
using Directory = Lucene.Net.Store.Directory;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Version = Lucene.Net.Util.Version;

namespace Raven.Database.Indexing
{
	/// <summary>
	/// 	This is a thread safe, single instance for a particular index.
	/// </summary>
	public abstract class Index : IDisposable
	{
		protected static readonly ILog logIndexing = LogManager.GetLogger(typeof(Index).FullName + ".Indexing");
		protected static readonly ILog logQuerying = LogManager.GetLogger(typeof(Index).FullName + ".Querying");
		private readonly List<Document> currentlyIndexDocuments = new List<Document>();
		protected Directory directory;
		protected readonly IndexDefinition indexDefinition;
		private volatile string waitReason;

		public IndexingPriority Priority { get; set; }

		/// <summary>
		/// Note, this might be written to be multiple threads at the same time
		/// We don't actually care for exact timing, it is more about general feeling
		/// </summary>
		private DateTime? lastQueryTime;

		private readonly ConcurrentDictionary<string, IIndexExtension> indexExtensions =
			new ConcurrentDictionary<string, IIndexExtension>();

		internal readonly int indexId;

	    public int IndexId
	    {
	        get { return indexId; }
	    }

		private readonly AbstractViewGenerator viewGenerator;
		protected readonly WorkContext context;
		private readonly object writeLock = new object();
		private volatile bool disposed;
		private RavenIndexWriter indexWriter;
		private SnapshotDeletionPolicy snapshotter;
		private readonly IndexSearcherHolder currentIndexSearcherHolder;

		private readonly ConcurrentDictionary<string, IndexingPerformanceStats> currentlyIndexing = new ConcurrentDictionary<string, IndexingPerformanceStats>();
		private readonly ConcurrentQueue<IndexingPerformanceStats> indexingPerformanceStats = new ConcurrentQueue<IndexingPerformanceStats>();
		private readonly static StopAnalyzer stopAnalyzer = new StopAnalyzer(Version.LUCENE_30);
		private bool forceWriteToDisk;

		protected Index(Directory directory, int id, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator, WorkContext context)
		{
		    currentIndexSearcherHolder = new IndexSearcherHolder(id ,context);
		    if (directory == null) throw new ArgumentNullException("directory");
			if (indexDefinition == null) throw new ArgumentNullException("indexDefinition");
			if (viewGenerator == null) throw new ArgumentNullException("viewGenerator");

			this.indexId = id;
			this.indexDefinition = indexDefinition;
			this.viewGenerator = viewGenerator;
			this.context = context;
			logIndexing.Debug("Creating index for {0}", indexId);
			this.directory = directory;

			RecreateSearcher();
		}

		[ImportMany]
		public OrderedPartCollection<AbstractAnalyzerGenerator> AnalyzerGenerators { get; set; }

		/// <summary>
		/// Whatever this is a map reduce index or not
		/// </summary>
		public abstract bool IsMapReduce { get; }

		public DateTime? LastQueryTime
		{
			get
			{
				return lastQueryTime;
			}
		}

		public DateTime LastIndexTime { get; set; }

		protected DateTime PreviousIndexTime { get; set; }

		public string IsOnRam
		{
			get
			{
				var ramDirectory = directory as RAMDirectory;
				if (ramDirectory == null)
					return "false";
				try
				{
                    return "true (" + SizeHelper.Humane(ramDirectory.SizeInBytes()) + ")";
				}
				catch (AlreadyClosedException)
				{
					return "false";
				}
			}
		}

	    public string PublicName { get { return this.indexDefinition.Name; } }

		public volatile bool IsMapIndexingInProgress;

		protected void RecordCurrentBatch(string indexingStep, int size)
		{
			var performanceStats = new IndexingPerformanceStats
			{
				InputCount = size, 
				Operation = indexingStep,
				Started = SystemTime.UtcNow,
			};
			currentlyIndexing.AddOrUpdate(indexingStep, performanceStats, (s, stats) => performanceStats);
		}

		protected void BatchCompleted(string indexingStep)
		{
			IndexingPerformanceStats value;
			currentlyIndexing.TryRemove(indexingStep, out value);
		}

		protected void AddindexingPerformanceStat(IndexingPerformanceStats stats)
		{
			indexingPerformanceStats.Enqueue(stats);
			while (indexingPerformanceStats.Count > 25)
				indexingPerformanceStats.TryDequeue(out stats);
		}

		public void Dispose()
		{
			try
			{
				// this is here so we can give good logs in the case of a long shutdown process
				if (Monitor.TryEnter(writeLock, 100) == false)
				{
					var localReason = waitReason;
					if (localReason != null)
						logIndexing.Warn("Waiting for {0} to complete before disposing of index {1}, that might take a while if the server is very busy",
						 localReason, indexId);

					Monitor.Enter(writeLock);
				}

				disposed = true;

				foreach (var indexExtension in indexExtensions)
				{
					indexExtension.Value.Dispose();
				}

				if (currentIndexSearcherHolder != null)
				{
					var item = currentIndexSearcherHolder.SetIndexSearcher(null, wait: true);
					if (item.WaitOne(TimeSpan.FromSeconds(5)) == false)
					{
						logIndexing.Warn("After closing the index searching, we waited for 5 seconds for the searching to be done, but it wasn't. Continuing with normal shutdown anyway.");
					}
				}

				if (indexWriter != null)
				{
					var writer = indexWriter;
					indexWriter = null;

					try
					{
						writer.Analyzer.Close();
					}
					catch (Exception e)
					{
						logIndexing.ErrorException("Error while closing the index (closing the analyzer failed)", e);
					}

					try
					{
						writer.Dispose();
					}
					catch (Exception e)
					{
						logIndexing.ErrorException("Error when closing the index", e);
					}
				}

				try
				{
					directory.Dispose();
				}
				catch (Exception e)
				{
					logIndexing.ErrorException("Error when closing the directory", e);
				}
			}
			finally
			{
				Monitor.Exit(writeLock);
			}
		}

		public void Flush()
		{
			lock (writeLock)
			{
				if (disposed)
					return;
				if (indexWriter == null)
					return;

				try
				{
					waitReason = "Flush";
					indexWriter.Commit();
				}
				finally
				{
					waitReason = null;
				}
			}
		}

		public void MergeSegments()
		{
			lock (writeLock)
			{
				waitReason = "Merge / Optimize";
				try
				{
					logIndexing.Info("Starting merge of {0}", indexId);
					var sp = Stopwatch.StartNew();
					indexWriter.Optimize();
					logIndexing.Info("Done merging {0} - took {1}", indexId, sp.Elapsed);
				}
				finally
				{
					waitReason = null;
				}
			}
		}

		public abstract void IndexDocuments(AbstractViewGenerator viewGenerator, IndexingBatch batch, IStorageActionsAccessor actions, DateTime minimumTimestamp);

		protected virtual IndexQueryResult RetrieveDocument(Document document, FieldsToFetch fieldsToFetch, ScoreDoc score)
		{
			return new IndexQueryResult
			{
				Score = score.Score,
				Key = document.Get(Constants.DocumentIdFieldName),
				Projection = (fieldsToFetch.IsProjection || fieldsToFetch.FetchAllStoredFields) ? CreateDocumentFromFields(document, fieldsToFetch) : null
			};
		}

		public static RavenJObject CreateDocumentFromFields(Document document, FieldsToFetch fieldsToFetch)
		{
			var documentFromFields = new RavenJObject();
			var fields = fieldsToFetch.Fields;
			if (fieldsToFetch.FetchAllStoredFields)
				fields = fields.Concat(document.GetFields().Select(x => x.Name));


			var q = fields
				.Distinct()
				.SelectMany(name => document.GetFields(name) ?? new Field[0])
				.Where(x => x != null)
				.Where(
					x =>
					x.Name.EndsWith("_IsArray") == false &&
					x.Name.EndsWith("_Range") == false &&
					x.Name.EndsWith("_ConvertToJson") == false)
				.Select(fld => CreateProperty(fld, document))
				.GroupBy(x => x.Key)
				.Select(g =>
				{
					if (g.Count() == 1 && document.GetField(g.Key + "_IsArray") == null)
					{
						return g.First();
					}
					var ravenJTokens = g.Select(x => x.Value).ToArray();
					return new KeyValuePair<string, RavenJToken>(g.Key, new RavenJArray((IEnumerable)ravenJTokens));
				});
			foreach (var keyValuePair in q)
			{
				documentFromFields.Add(keyValuePair.Key, keyValuePair.Value);
			}
			return documentFromFields;
		}

        protected void InvokeOnIndexEntryDeletedOnAllBatchers(List<AbstractIndexUpdateTriggerBatcher> batchers, Term term)
        {
            if (!batchers.Any(batcher => batcher.RequiresDocumentOnIndexEntryDeleted)) return;
            // find all documents
            var key = term.Text;

            IndexSearcher searcher = null;
            using (GetSearcher(out searcher))
            {
                var collector = new GatherAllCollector();
                searcher.Search(new TermQuery(term), collector);
                var topDocs = collector.ToTopDocs();
                
                foreach (var scoreDoc in topDocs.ScoreDocs)
                {
                    var document = searcher.Doc(scoreDoc.Doc);
                    batchers.ApplyAndIgnoreAllErrors(
                        exception =>
                        {
                            logIndexing.WarnException(
                                string.Format(
                                    "Error when executed OnIndexEntryDeleted trigger for index '{0}', key: '{1}'",
                                    indexId, key),
                                exception);
                            context.AddError(indexId, key, exception.Message, "OnIndexEntryDeleted Trigger");
                        },
                        trigger => trigger.OnIndexEntryDeleted(key, document));
                }
            }
        }

		private static KeyValuePair<string, RavenJToken> CreateProperty(Field fld, Document document)
		{
			if (fld.IsBinary)
				return new KeyValuePair<string, RavenJToken>(fld.Name, fld.GetBinaryValue());
			var stringValue = fld.StringValue;
			if (document.GetField(fld.Name + "_ConvertToJson") != null)
			{
				var val = RavenJToken.Parse(fld.StringValue) as RavenJObject;
				return new KeyValuePair<string, RavenJToken>(fld.Name, val);
			}
			if (stringValue == Constants.NullValue)
				stringValue = null;
			if (stringValue == Constants.EmptyString)
				stringValue = string.Empty;
			return new KeyValuePair<string, RavenJToken>(fld.Name, stringValue);
		}

		protected void Write(Func<RavenIndexWriter, Analyzer, IndexingWorkStats, IndexedItemsInfo> action)
		{
			if (disposed)
				throw new ObjectDisposedException("Index " + indexId + " has been disposed");

			PreviousIndexTime = LastIndexTime;
			LastIndexTime = SystemTime.UtcNow;

			lock (writeLock)
			{
				bool shouldRecreateSearcher;
				var toDispose = new List<Action>();
				Analyzer searchAnalyzer = null;
				var itemsInfo = new IndexedItemsInfo();

				try
				{
					waitReason = "Write";
					try
					{
						searchAnalyzer = CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose);
					}
					catch (Exception e)
					{
						context.AddError(indexId, indexDefinition.Name, "Creating Analyzer", e.ToString(), "Analyzer");
						throw;
					}

					if (indexWriter == null)
					{
						CreateIndexWriter();
					}

					var locker = directory.MakeLock("writing-to-index.lock");
					try
					{
						var stats = new IndexingWorkStats();

						try
						{
							if (locker.Obtain() == false)
							{
			                    throw new InvalidOperationException(
			                        string.Format("Could not obtain the 'writing-to-index' lock of '{0}' index",
																				  indexId));
							}

							itemsInfo = action(indexWriter, searchAnalyzer, stats);
							shouldRecreateSearcher = itemsInfo.ChangedDocs > 0;
							foreach (var indexExtension in indexExtensions.Values)
							{
								indexExtension.OnDocumentsIndexed(currentlyIndexDocuments, searchAnalyzer);
							}
						}
						catch (Exception e)
						{
							context.AddError(indexId, indexDefinition.Name, null, e.ToString(), "Write");
							throw;
						}

						if (itemsInfo.ChangedDocs > 0)
						{
							UpdateIndexingStats(context, stats);
							WriteInMemoryIndexToDiskIfNecessary(itemsInfo.HighestETag);
							Flush(); // just make sure changes are flushed to disk
						}
					}
					finally
					{
						locker.Release();
					}
				}
			    catch (Exception e)
			    {
			        throw new InvalidOperationException("Could not properly write to index " + indexId, e);
			    }
				finally
				{
					currentlyIndexDocuments.Clear();
					if (searchAnalyzer != null)
						searchAnalyzer.Close();
					foreach (Action dispose in toDispose)
					{
						dispose();
					}
					waitReason = null;
					LastIndexTime = SystemTime.UtcNow;
				}

				try
				{
					HandleCommitPoints(itemsInfo);
				}
				catch (Exception e)
				{
					logIndexing.WarnException("Could not handle commit point properly, ignoring", e);
				}

				if (shouldRecreateSearcher)
					RecreateSearcher();
			}
		}

		protected abstract void HandleCommitPoints(IndexedItemsInfo itemsInfo);

		protected void UpdateIndexingStats(WorkContext workContext, IndexingWorkStats stats)
		{
			switch (stats.Operation)
			{
				case IndexingWorkStats.Status.Map:
					workContext.TransactionalStorage.Batch(accessor => accessor.Indexing.UpdateIndexingStats(indexId, stats));
					break;
				case IndexingWorkStats.Status.Reduce:
					workContext.TransactionalStorage.Batch(accessor => accessor.Indexing.UpdateReduceStats(indexId, stats));
					break;
				case IndexingWorkStats.Status.Ignore:
					break;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		private void CreateIndexWriter()
		{
			snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
		    IndexWriter.IndexReaderWarmer indexReaderWarmer = context.IndexReaderWarmers != null
		                                                          ? new IndexReaderWarmersWrapper(indexDefinition.Name, context.IndexReaderWarmers)
		                                                          : null;
			indexWriter = new RavenIndexWriter(directory, stopAnalyzer, snapshotter, IndexWriter.MaxFieldLength.UNLIMITED, context.Configuration.MaxIndexWritesBeforeRecreate, indexReaderWarmer);
		}

		private void WriteInMemoryIndexToDiskIfNecessary(Etag highestETag)
		{
			if (context.Configuration.RunInMemory ||
				context.IndexDefinitionStorage == null) // may happen during index startup
				return;

			var dir = indexWriter.Directory as RAMDirectory;
			if (dir == null)
				return;

			var stale = IsUpToDateEnoughToWriteToDisk(highestETag) == false;
			var toobig = dir.SizeInBytes() >= context.Configuration.NewIndexInMemoryMaxBytes;

			if (forceWriteToDisk || toobig || !stale)
			{
				indexWriter.Commit();
				var fsDir = context.IndexStorage.MakeRAMDirectoryPhysical(dir, indexDefinition);
				IndexStorage.WriteIndexVersion(fsDir, indexDefinition);
				directory = fsDir;

				indexWriter.Dispose(true);
				dir.Dispose();

				CreateIndexWriter();
			}
		}

		protected abstract bool IsUpToDateEnoughToWriteToDisk(Etag highestETag);

		public RavenPerFieldAnalyzerWrapper CreateAnalyzer(Analyzer defaultAnalyzer, ICollection<Action> toDispose, bool forQuerying = false)
		{
			toDispose.Add(defaultAnalyzer.Close);

			string value;
			if (indexDefinition.Analyzers.TryGetValue(Constants.AllFields, out value))
			{
				defaultAnalyzer = IndexingExtensions.CreateAnalyzerInstance(Constants.AllFields, value);
				toDispose.Add(defaultAnalyzer.Close);
			}
			var perFieldAnalyzerWrapper = new RavenPerFieldAnalyzerWrapper(defaultAnalyzer);
			foreach (var analyzer in indexDefinition.Analyzers)
			{
				Analyzer analyzerInstance = IndexingExtensions.CreateAnalyzerInstance(analyzer.Key, analyzer.Value);
				toDispose.Add(analyzerInstance.Close);

				if (forQuerying)
				{
					var customAttributes = analyzerInstance.GetType().GetCustomAttributes(typeof(NotForQueryingAttribute), false);
					if (customAttributes.Length > 0)
						continue;
				}

				perFieldAnalyzerWrapper.AddAnalyzer(analyzer.Key, analyzerInstance);
			}
			StandardAnalyzer standardAnalyzer = null;
			KeywordAnalyzer keywordAnalyzer = null;
			foreach (var fieldIndexing in indexDefinition.Indexes)
			{
				switch (fieldIndexing.Value)
				{
					case FieldIndexing.NotAnalyzed:
						if (keywordAnalyzer == null)
						{
							keywordAnalyzer = new KeywordAnalyzer();
							toDispose.Add(keywordAnalyzer.Close);
						}
						perFieldAnalyzerWrapper.AddAnalyzer(fieldIndexing.Key, keywordAnalyzer);
						break;
					case FieldIndexing.Analyzed:
						if (indexDefinition.Analyzers.ContainsKey(fieldIndexing.Key))
							continue;
						if (standardAnalyzer == null)
						{
							standardAnalyzer = new StandardAnalyzer(Version.LUCENE_29);
							toDispose.Add(standardAnalyzer.Close);
						}
						perFieldAnalyzerWrapper.AddAnalyzer(fieldIndexing.Key, standardAnalyzer);
						break;
				}
			}
			return perFieldAnalyzerWrapper;
		}

		protected IEnumerable<object> RobustEnumerationIndex(IEnumerator<object> input, IEnumerable<IndexingFunc> funcs, IndexingWorkStats stats)
		{
			return new RobustEnumerator(context.CancellationToken, context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
			{
				BeforeMoveNext = () => Interlocked.Increment(ref stats.IndexingAttempts),
				CancelMoveNext = () => Interlocked.Decrement(ref stats.IndexingAttempts),
				OnError = (exception, o) =>
				{
					context.AddError(indexId,
                                     indexDefinition.Name,
									TryGetDocKey(o),
									exception.Message,
									"Map"
						);
					logIndexing.WarnException(
						String.Format("Failed to execute indexing function on {0} on {1}", indexId,
										TryGetDocKey(o)),
						exception);

					stats.IndexingErrors++;
				}
			}.RobustEnumeration(input, funcs);
		}

		protected IEnumerable<object> RobustEnumerationReduce(IEnumerator<object> input, IndexingFunc func,
															IStorageActionsAccessor actions,
			IndexingWorkStats stats)
		{
			// not strictly accurate, but if we get that many errors, probably an error anyway.
			return new RobustEnumerator(context.CancellationToken, context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
			{
				BeforeMoveNext = () => Interlocked.Increment(ref stats.ReduceAttempts),
				CancelMoveNext = () => Interlocked.Decrement(ref stats.ReduceAttempts),
				OnError = (exception, o) =>
				{
					context.AddError(indexId,
                                     indexDefinition.Name,
									TryGetDocKey(o),
									exception.Message,
									"Reduce"
						);
					logIndexing.WarnException(
						String.Format("Failed to execute indexing function on {0} on {1}", indexId,
										TryGetDocKey(o)),
						exception);

					stats.ReduceErrors++;
				}
			}.RobustEnumeration(input, func);
		}

		// we don't care about tracking map/reduce stats here, since it is merely
		// an optimization step
		protected IEnumerable<object> RobustEnumerationReduceDuringMapPhase(IEnumerator<object> input, IndexingFunc func)
		{
			// not strictly accurate, but if we get that many errors, probably an error anyway.
			return new RobustEnumerator(context.CancellationToken, context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
			{
				BeforeMoveNext = () => { }, // don't care
				CancelMoveNext = () => { }, // don't care
				OnError = (exception, o) =>
				{
					context.AddError(indexId,
                                     indexDefinition.Name,
									TryGetDocKey(o),
									exception.Message,
									"Reduce"
						);
					logIndexing.WarnException(
						String.Format("Failed to execute indexing function on {0} on {1}", indexId,
										TryGetDocKey(o)),
						exception);
				}
			}.RobustEnumeration(input, func);
		}

		public static string TryGetDocKey(object current)
		{
			var dic = current as DynamicJsonObject;
			if (dic == null)
				return null;
			object value = dic.GetValue(Constants.DocumentIdFieldName);
			if (value == null)
				return null;
			return value.ToString();
		}

		public abstract void Remove(string[] keys, WorkContext context);

		internal IndexSearcherHolder.IndexSearcherHoldingState GetCurrentStateHolder()
		{
			return currentIndexSearcherHolder.GetCurrentStateHolder();
		}

		internal IDisposable GetSearcher(out IndexSearcher searcher)
		{
			return currentIndexSearcherHolder.GetSearcher(out searcher);
		}

		internal IDisposable GetSearcherAndTermsDocs(out IndexSearcher searcher, out RavenJObject[] termsDocs)
		{
			return currentIndexSearcherHolder.GetSearcherAndTermDocs(out searcher, out termsDocs);
		}

		private void RecreateSearcher()
		{
			if (indexWriter == null)
			{
				currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(directory, true), wait: false);
			}
			else
			{
				var indexReader = indexWriter.GetReader();
				currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(indexReader), wait: false);
			}
		}

		protected void AddDocumentToIndex(RavenIndexWriter currentIndexWriter, Document luceneDoc, Analyzer analyzer)
		{
			Analyzer newAnalyzer = AnalyzerGenerators.Aggregate(analyzer,
																(currentAnalyzer, generator) =>
																{
																	Analyzer generateAnalyzer =
																		generator.Value.GenerateAnalyzerForIndexing(indexId.ToString(), luceneDoc,
																											currentAnalyzer);
																	if (generateAnalyzer != currentAnalyzer &&
																		currentAnalyzer != analyzer)
																		currentAnalyzer.Close();
																	return generateAnalyzer;
																});

			try
			{
				if (indexExtensions.Count > 0)
					currentlyIndexDocuments.Add(CloneDocument(luceneDoc));

				currentIndexWriter.AddDocument(luceneDoc, newAnalyzer);

				foreach (var fieldable in luceneDoc.GetFields())
				{
					using (fieldable.ReaderValue) // dispose all the readers
					{
						
					}
				}
			}
			finally
			{
				if (newAnalyzer != analyzer)
					newAnalyzer.Close();
			}
		}

		public void MarkQueried()
		{
			lastQueryTime = SystemTime.UtcNow;
		}

		public void MarkQueried(DateTime time)
		{
			lastQueryTime = time;
		}

		public IIndexExtension GetExtension(string indexExtensionKey)
		{
			IIndexExtension val;
			indexExtensions.TryGetValue(indexExtensionKey, out val);
			return val;
		}

		public IIndexExtension GetExtensionByPrefix(string indexExtensionKeyPrefix)
		{
			return indexExtensions.FirstOrDefault(x => x.Key.StartsWith(indexExtensionKeyPrefix)).Value;
		}

		public void SetExtension(string indexExtensionKey, IIndexExtension extension)
		{
			indexExtensions.TryAdd(indexExtensionKey, extension);
		}

		private static Document CloneDocument(Document luceneDoc)
		{
			var clonedDocument = new Document();
			foreach (AbstractField field in luceneDoc.GetFields())
			{
				var numericField = field as NumericField;
				if (numericField != null)
				{
					var clonedNumericField = new NumericField(numericField.Name,
															numericField.IsStored ? Field.Store.YES : Field.Store.NO,
															numericField.IsIndexed);
					var numericValue = numericField.NumericValue;
					if (numericValue is int)
					{
						clonedNumericField.SetIntValue((int)numericValue);
					}
					else if (numericValue is long)
					{
						clonedNumericField.SetLongValue((long)numericValue);
					}
					else if (numericValue is double)
					{
						clonedNumericField.SetDoubleValue((double)numericValue);
					}
					else if (numericValue is float)
					{
						clonedNumericField.SetFloatValue((float)numericValue);
					}
					clonedDocument.Add(clonedNumericField);
				}
				else
				{
					Field clonedField;
					if (field.IsBinary)
					{
						clonedField = new Field(field.Name, field.GetBinaryValue(),
												field.IsStored ? Field.Store.YES : Field.Store.NO);
					}
					else if (field.StringValue != null)
					{
						clonedField = new Field(field.Name, field.StringValue,
												field.IsStored ? Field.Store.YES : Field.Store.NO,
												field.IsIndexed ? Field.Index.ANALYZED_NO_NORMS : Field.Index.NOT_ANALYZED_NO_NORMS,
												field.IsTermVectorStored ? Field.TermVector.YES : Field.TermVector.NO);
					}
					else
					{
						//probably token stream, and we can't handle fields with token streams, so we skip this.
						continue;
					}
					clonedDocument.Add(clonedField);
				}
			}
			return clonedDocument;
		}

		protected void LogIndexedDocument(string key, Document luceneDoc)
		{
			if (logIndexing.IsDebugEnabled)
			{
				var fieldsForLogging = luceneDoc.GetFields().Cast<IFieldable>().Select(x => new
				{
					Name = x.Name,
					Value = x.IsBinary ? "<binary>" : x.StringValue,
					Indexed = x.IsIndexed,
					Stored = x.IsStored,
				});
				var sb = new StringBuilder();
				foreach (var fieldForLogging in fieldsForLogging)
				{
					sb.Append("\t").Append(fieldForLogging.Name)
						.Append(" ")
						.Append(fieldForLogging.Indexed ? "I" : "-")
						.Append(fieldForLogging.Stored ? "S" : "-")
						.Append(": ")
						.Append(fieldForLogging.Value)
						.AppendLine();
				}

				logIndexing.Debug("Indexing on {0} result in index {1} gave document: {2}", key, indexId,
								sb.ToString());
			}
		}

	    public static void AssertQueryDoesNotContainFieldsThatAreNotIndexed(IndexQuery indexQuery, AbstractViewGenerator viewGenerator)
        {
		    if (string.IsNullOrWhiteSpace(indexQuery.Query) == false)
		    {
			    HashSet<string> hashSet = SimpleQueryParser.GetFields(indexQuery);
			    foreach (string field in hashSet)
			    {
				    string f = field;
				    if (f.EndsWith("_Range"))
				    {
					    f = f.Substring(0, f.Length - "_Range".Length);
				    }
				    if (viewGenerator.ContainsField(f) == false &&
				        viewGenerator.ContainsField("_") == false) // the catch all field name means that we have dynamic fields names
					    throw new ArgumentException("The field '" + f + "' is not indexed, cannot query on fields that are not indexed");
			    }
		    }
		    if (indexQuery.SortedFields != null)
		    {
			    foreach (SortedField field in indexQuery.SortedFields)
			    {
				    string f = field.Field;
				    if (f == Constants.TemporaryScoreValue)
					    continue;
				    if (f.EndsWith("_Range"))
				    {
					    f = f.Substring(0, f.Length - "_Range".Length);
				    }
				    if (f.StartsWith(Constants.RandomFieldName))
					    continue;
				    if (viewGenerator.ContainsField(f) == false && f != Constants.DistanceFieldName
				        && viewGenerator.ContainsField("_") == false) // the catch all field name means that we have dynamic fields names
					    throw new ArgumentException("The field '" + f + "' is not indexed, cannot sort on fields that are not indexed");
			    }
		    }
        }



		#region Nested type: IndexQueryOperation

		internal class IndexQueryOperation
		{
			FastVectorHighlighter highlighter;
			FieldQuery fieldQuery;

			private readonly IndexQuery indexQuery;
			private readonly Index parent;
			private readonly Func<IndexQueryResult, bool> shouldIncludeInResults;
			private readonly HashSet<RavenJObject> alreadyReturned;
			private readonly FieldsToFetch fieldsToFetch;
			private readonly HashSet<string> documentsAlreadySeenInPreviousPage = new HashSet<string>();
			private readonly OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers;

			public IndexQueryOperation(Index parent, IndexQuery indexQuery, Func<IndexQueryResult, bool> shouldIncludeInResults,
										FieldsToFetch fieldsToFetch, OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers)
			{
				this.parent = parent;
				this.indexQuery = indexQuery;
				this.shouldIncludeInResults = shouldIncludeInResults;
				this.fieldsToFetch = fieldsToFetch;
				this.indexQueryTriggers = indexQueryTriggers;

				if (fieldsToFetch.IsDistinctQuery)
					alreadyReturned = new HashSet<RavenJObject>(new RavenJTokenEqualityComparer());
			}

			public IEnumerable<RavenJObject> IndexEntries(Reference<int> totalResults)
			{
				parent.MarkQueried();
				using (IndexStorage.EnsureInvariantCulture())
				{
					AssertQueryDoesNotContainFieldsThatAreNotIndexed(indexQuery, parent.viewGenerator);
					IndexSearcher indexSearcher;
					RavenJObject[] termsDocs;
					using (parent.GetSearcherAndTermsDocs(out indexSearcher, out termsDocs))
					{
                        var documentQuery = GetDocumentQuery();

						TopDocs search = ExecuteQuery(indexSearcher, documentQuery, indexQuery.Start, indexQuery.PageSize, indexQuery);
						totalResults.Value = search.TotalHits;

						for (int index = indexQuery.Start; index < search.ScoreDocs.Length; index++)
						{
							var scoreDoc = search.ScoreDocs[index];
							var ravenJObject = (RavenJObject)termsDocs[scoreDoc.Doc].CloneToken();
							foreach (var prop in ravenJObject.Where(x => x.Key.EndsWith("_Range")).ToArray())
							{
								ravenJObject.Remove(prop.Key);
							}
							yield return ravenJObject;
						}
					}
				}
			}

			public IEnumerable<IndexQueryResult> Query(CancellationToken token)
			{
			    if (parent.Priority.HasFlag(IndexingPriority.Error))
			        throw new IndexDisabledException("The index has been disabled due to errors");

				parent.MarkQueried();
				using (IndexStorage.EnsureInvariantCulture())
				{
					AssertQueryDoesNotContainFieldsThatAreNotIndexed(indexQuery, parent.viewGenerator);
					IndexSearcher indexSearcher;
					using (parent.GetSearcher(out indexSearcher))
					{
						var documentQuery = GetDocumentQuery();


						int start = indexQuery.Start;
						int pageSize = indexQuery.PageSize;
						int returnedResults = 0;
						int skippedResultsInCurrentLoop = 0;
						bool readAll;
						bool adjustStart = true;
						DuplicateDocumentRecorder recorder = null;
						if (indexQuery.SkipDuplicateChecking == false)
							recorder = new DuplicateDocumentRecorder(indexSearcher, parent, documentsAlreadySeenInPreviousPage,
								alreadyReturned, fieldsToFetch, parent.IsMapReduce || fieldsToFetch.IsProjection);

						do
						{
							if (skippedResultsInCurrentLoop > 0)
							{
								start = start + pageSize - (start - indexQuery.Start); // need to "undo" the index adjustment
								// trying to guesstimate how many results we will need to read from the index
								// to get enough unique documents to match the page size
								pageSize = Math.Max(2, skippedResultsInCurrentLoop) * pageSize;
								skippedResultsInCurrentLoop = 0;
							}
							TopDocs search;
							int moreRequired = 0;
							do
							{
								token.ThrowIfCancellationRequested(); 
								search = ExecuteQuery(indexSearcher, documentQuery, start, pageSize, indexQuery);

								if (recorder != null)
								{
									moreRequired = recorder.RecordResultsAlreadySeenForDistinctQuery(search, adjustStart, pageSize, ref start);
									pageSize += moreRequired*2;
								}
							} while (moreRequired > 0);

							indexQuery.TotalSize.Value = search.TotalHits;
							adjustStart = false;

							SetupHighlighter(documentQuery);

							for (var i = start; (i - start) < pageSize && i < search.ScoreDocs.Length; i++)
							{
								var scoreDoc = search.ScoreDocs[i];
								var document = indexSearcher.Doc(scoreDoc.Doc);
								var indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, scoreDoc);
								if (ShouldIncludeInResults(indexQueryResult) == false)
								{
									indexQuery.SkippedResults.Value++;
									skippedResultsInCurrentLoop++;
									continue;
								}

								AddHighlighterResults(indexSearcher, scoreDoc, indexQueryResult);

								AddQueryExplanation(documentQuery, indexSearcher, scoreDoc, indexQueryResult);

								returnedResults++;
								yield return indexQueryResult;
								if (returnedResults == indexQuery.PageSize)
									yield break;
							}
							readAll = search.TotalHits == search.ScoreDocs.Length;
						} while (returnedResults < indexQuery.PageSize && readAll == false);
					}
				}
			}

			private void AddHighlighterResults(IndexSearcher indexSearcher, ScoreDoc scoreDoc, IndexQueryResult indexQueryResult)
			{
				if (highlighter == null)
					return;

				var highlightings =
					from highlightedField in this.indexQuery.HighlightedFields
					select new
					{
						highlightedField.Field,
						highlightedField.FragmentsField,
						Fragments = highlighter.GetBestFragments(
							fieldQuery,
							indexSearcher.IndexReader,
							scoreDoc.Doc,
							highlightedField.Field,
							highlightedField.FragmentLength,
							highlightedField.FragmentCount)
					}
						into fieldHighlitings
						where fieldHighlitings.Fragments != null &&
							  fieldHighlitings.Fragments.Length > 0
						select fieldHighlitings;

				if (fieldsToFetch.IsProjection || parent.IsMapReduce)
				{
					foreach (var highlighting in highlightings)
					{
						if (!string.IsNullOrEmpty(highlighting.FragmentsField))
						{
							indexQueryResult.Projection[highlighting.FragmentsField] = new RavenJArray(highlighting.Fragments);
						}
					}
				}
				else
				{
					indexQueryResult.Highligtings = highlightings.ToDictionary(x => x.Field, x => x.Fragments);
				}
			}

			private void SetupHighlighter(Query documentQuery)
			{
				if (indexQuery.HighlightedFields != null && indexQuery.HighlightedFields.Length > 0)
				{
					highlighter = new FastVectorHighlighter(
						FastVectorHighlighter.DEFAULT_PHRASE_HIGHLIGHT,
						FastVectorHighlighter.DEFAULT_FIELD_MATCH,
						new SimpleFragListBuilder(),
						new SimpleFragmentsBuilder(
							indexQuery.HighlighterPreTags != null && indexQuery.HighlighterPreTags.Any()
								? indexQuery.HighlighterPreTags
								: BaseFragmentsBuilder.COLORED_PRE_TAGS,
							indexQuery.HighlighterPostTags != null && indexQuery.HighlighterPostTags.Any()
								? indexQuery.HighlighterPostTags
								: BaseFragmentsBuilder.COLORED_POST_TAGS));

					fieldQuery = highlighter.GetFieldQuery(documentQuery);
				}
			}

			private void AddQueryExplanation(Query documentQuery, IndexSearcher indexSearcher, ScoreDoc scoreDoc, IndexQueryResult indexQueryResult)
			{
				if(indexQuery.ExplainScores == false)
					return;

				var explanation = indexSearcher.Explain(documentQuery, scoreDoc.Doc);

				indexQueryResult.ScoreExplanation = explanation.ToString();
			}

			private Query ApplyIndexTriggers(Query documentQuery)
			{
				documentQuery = indexQueryTriggers.Aggregate(documentQuery,
														   (current, indexQueryTrigger) =>
														   indexQueryTrigger.Value.ProcessQuery(parent.indexId.ToString(), current, indexQuery));
				return documentQuery;
			}

			public IEnumerable<IndexQueryResult> IntersectionQuery(CancellationToken token)
			{
				using (IndexStorage.EnsureInvariantCulture())
				{
					AssertQueryDoesNotContainFieldsThatAreNotIndexed(indexQuery, parent.viewGenerator);
					IndexSearcher indexSearcher;
					using (parent.GetSearcher(out indexSearcher))
					{
						var subQueries = indexQuery.Query.Split(new[] { Constants.IntersectSeparator }, StringSplitOptions.RemoveEmptyEntries);
						if (subQueries.Length <= 1)
							throw new InvalidOperationException("Invalid INTERSECT query, must have multiple intersect clauses.");

						//Not sure how to select the page size here??? The problem is that only docs in this search can be part 
						//of the final result because we're doing an intersection query (but we might exclude some of them)
						int pageSizeBestGuess = (indexQuery.Start + indexQuery.PageSize) * 2;
						int intersectMatches = 0, skippedResultsInCurrentLoop = 0;
						int previousBaseQueryMatches = 0, currentBaseQueryMatches = 0;

                        var firstSubDocumentQuery = GetDocumentQuery(subQueries[0], indexQuery);

						//Do the first sub-query in the normal way, so that sorting, filtering etc is accounted for
						var search = ExecuteQuery(indexSearcher, firstSubDocumentQuery, 0, pageSizeBestGuess, indexQuery);
						currentBaseQueryMatches = search.ScoreDocs.Length;
						var intersectionCollector = new IntersectionCollector(indexSearcher, search.ScoreDocs);

						do
						{
							token.ThrowIfCancellationRequested();
							if (skippedResultsInCurrentLoop > 0)
							{
								// We get here because out first attempt didn't get enough docs (after INTERSECTION was calculated)
								pageSizeBestGuess = pageSizeBestGuess * 2;

								search = ExecuteQuery(indexSearcher, firstSubDocumentQuery, 0, pageSizeBestGuess, indexQuery);
								previousBaseQueryMatches = currentBaseQueryMatches;
								currentBaseQueryMatches = search.ScoreDocs.Length;
								intersectionCollector = new IntersectionCollector(indexSearcher, search.ScoreDocs);
							}

							for (int i = 1; i < subQueries.Length; i++)
							{
								var luceneSubQuery = GetDocumentQuery(subQueries[i], indexQuery);
								indexSearcher.Search(luceneSubQuery, null, intersectionCollector);
							}

							var currentIntersectResults = intersectionCollector.DocumentsIdsForCount(subQueries.Length).ToList();
							intersectMatches = currentIntersectResults.Count;
							skippedResultsInCurrentLoop = pageSizeBestGuess - intersectMatches;
						} while (intersectMatches < indexQuery.PageSize && //stop if we've got enough results to satisfy the pageSize
								 currentBaseQueryMatches < search.TotalHits && //stop if increasing the page size wouldn't make any difference
								 previousBaseQueryMatches < currentBaseQueryMatches); //stop if increasing the page size didn't result in any more "base query" results

						var intersectResults = intersectionCollector.DocumentsIdsForCount(subQueries.Length).ToList();
						//It's hard to know what to do here, the TotalHits from the base search isn't really the TotalSize, 
						//because it's before the INTERSECTION has been applied, so only some of those results make it out.
						//Trying to give an accurate answer is going to be too costly, so we aren't going to try.
						indexQuery.TotalSize.Value = search.TotalHits;
						indexQuery.SkippedResults.Value = skippedResultsInCurrentLoop;

						//Using the final set of results in the intersectionCollector
						int returnedResults = 0;
						for (int i = indexQuery.Start; i < intersectResults.Count && (i - indexQuery.Start) < pageSizeBestGuess; i++)
						{
							Document document = indexSearcher.Doc(intersectResults[i].LuceneId);
							IndexQueryResult indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, search.ScoreDocs[i]);
							if (ShouldIncludeInResults(indexQueryResult) == false)
							{
								indexQuery.SkippedResults.Value++;
								skippedResultsInCurrentLoop++;
								continue;
							}
							returnedResults++;
							yield return indexQueryResult;
							if (returnedResults == indexQuery.PageSize)
								yield break;
						}
					}
				}
			}

			private bool ShouldIncludeInResults(IndexQueryResult indexQueryResult)
			{
				if (shouldIncludeInResults(indexQueryResult) == false)
					return false;
				if (documentsAlreadySeenInPreviousPage.Contains(indexQueryResult.Key))
					return false;
				if (fieldsToFetch.IsDistinctQuery && alreadyReturned.Add(indexQueryResult.Projection) == false)
					return false;
				return true;
			}

			private void RecordResultsAlreadySeenForDistinctQuery(IndexSearcher indexSearcher, TopDocs search, int start, int pageSize)
			{
				var min = Math.Min(start, search.TotalHits);

				// we are paging, we need to check that we don't have duplicates in the previous page
				// see here for details: http://groups.google.com/group/ravendb/browse_frm/thread/d71c44aa9e2a7c6e
				if (parent.IsMapReduce == false && fieldsToFetch.IsProjection == false && start - pageSize >= 0 && start < search.TotalHits)
				{
					for (int i = start - pageSize; i < min; i++)
					{
						var document = indexSearcher.Doc(search.ScoreDocs[i].Doc);
						documentsAlreadySeenInPreviousPage.Add(document.Get(Constants.DocumentIdFieldName));
					}
				}

				if (fieldsToFetch.IsDistinctQuery == false)
					return;

				// add results that were already there in previous pages
				for (int i = 0; i < min; i++)
				{
					Document document = indexSearcher.Doc(search.ScoreDocs[i].Doc);
					var indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, search.ScoreDocs[i]);
					alreadyReturned.Add(indexQueryResult.Projection);
				}
			}

			public Query GetDocumentQuery()
			{
				var q = GetDocumentQuery(indexQuery.Query, indexQuery);
				var spatialIndexQuery = indexQuery as SpatialIndexQuery;
				if (spatialIndexQuery != null)
				{
					var spatialField = parent.viewGenerator.GetSpatialField(spatialIndexQuery.SpatialFieldName);
					var dq = spatialField.MakeQuery(q, spatialField.GetStrategy(), spatialIndexQuery);
					if (q is MatchAllDocsQuery) return dq;

					var bq = new BooleanQuery { { q, Occur.MUST }, { dq, Occur.MUST } };
					return bq;
				}
				return q;
			}

			private Query GetDocumentQuery(string query, IndexQuery indexQuery)
			{
				Query documentQuery;
				if (String.IsNullOrEmpty(query))
				{
					logQuerying.Debug("Issuing query on index {0} for all documents", parent.indexId);
					documentQuery = new MatchAllDocsQuery();
				}
				else
				{
					logQuerying.Debug("Issuing query on index {0} for: {1}", parent.indexId, query);
					var toDispose = new List<Action>();
					RavenPerFieldAnalyzerWrapper searchAnalyzer = null;
					try
					{
						searchAnalyzer = parent.CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose, true);
						searchAnalyzer = parent.AnalyzerGenerators.Aggregate(searchAnalyzer, (currentAnalyzer, generator) =>
						{
							Analyzer newAnalyzer = generator.GenerateAnalyzerForQuerying(parent.indexId.ToString(), indexQuery.Query, currentAnalyzer);
							if (newAnalyzer != currentAnalyzer)
							{
								DisposeAnalyzerAndFriends(toDispose, currentAnalyzer);
							}
							return parent.CreateAnalyzer(newAnalyzer, toDispose, true);
						});
						documentQuery = QueryBuilder.BuildQuery(query, indexQuery, searchAnalyzer);
					}
					finally
					{
						DisposeAnalyzerAndFriends(toDispose, searchAnalyzer);
					}
				}
				return ApplyIndexTriggers(documentQuery);
			}

			private static void DisposeAnalyzerAndFriends(List<Action> toDispose, RavenPerFieldAnalyzerWrapper analyzer)
			{
				if (analyzer != null)
					analyzer.Close();
				foreach (Action dispose in toDispose)
				{
					dispose();
				}
				toDispose.Clear();
			}

			private TopDocs ExecuteQuery(IndexSearcher indexSearcher, Query documentQuery, int start, int pageSize,
										IndexQuery indexQuery)
			{
				var sort = indexQuery.GetSort(parent.indexDefinition, parent.viewGenerator);

				if (pageSize == Int32.MaxValue && sort == null) // we want all docs, no sorting required
				{
					var gatherAllCollector = new GatherAllCollector();
					indexSearcher.Search(documentQuery, gatherAllCollector);
					return gatherAllCollector.ToTopDocs();
				}
			    int absFullPage = Math.Abs(pageSize + start); // need to protect against ridiculously high values of pageSize + start that overflow
			    var minPageSize = Math.Max(absFullPage, 1);

				// NOTE: We get Start + Pagesize results back so we have something to page on
				if (sort != null)
				{
					try
					{
						//indexSearcher.SetDefaultFieldSortScoring (sort.GetSort().Contains(SortField.FIELD_SCORE), false);
						indexSearcher.SetDefaultFieldSortScoring(true, false);
						var ret = indexSearcher.Search(documentQuery, null, minPageSize, sort);
						return ret;
					}
					finally
					{
						indexSearcher.SetDefaultFieldSortScoring(false, false);
					}
				}
				return indexSearcher.Search(documentQuery, null, minPageSize);
			}
		}

		#endregion

		public class DuplicateDocumentRecorder
		{
			private int min = -1;
			private readonly bool isProjectionOrMapReduce;
			private readonly Searchable indexSearcher;
			private readonly Index parent;
			private int alreadyScannedPositions, alreadyScannedPositionsForDistinct;
			private readonly HashSet<string> documentsAlreadySeenInPreviousPage;
			private readonly HashSet<RavenJObject> alreadyReturned;
			private readonly FieldsToFetch fieldsToFetch;

			public DuplicateDocumentRecorder(Searchable indexSearcher,
				Index parent,
				HashSet<string> documentsAlreadySeenInPreviousPage,
				HashSet<RavenJObject> alreadyReturned,
				FieldsToFetch fieldsToFetch,
				bool isProjectionOrMapReduce)
			{
				this.indexSearcher = indexSearcher;
				this.parent = parent;
				this.isProjectionOrMapReduce = isProjectionOrMapReduce;
				this.alreadyReturned = alreadyReturned;
				this.fieldsToFetch = fieldsToFetch;
				this.documentsAlreadySeenInPreviousPage = documentsAlreadySeenInPreviousPage;
			}


			public int RecordResultsAlreadySeenForDistinctQuery(TopDocs search, bool adjustStart, int pageSize, ref int start)
			{
				int itemsSkipped = 0;
				if (min == -1)
				{
					min = start;
				}
				min = Math.Min(min, search.TotalHits);

				// we are paging, we need to check that we don't have duplicates in the previous pages
				// see here for details: http://groups.google.com/group/ravendb/browse_frm/thread/d71c44aa9e2a7c6e
				if (isProjectionOrMapReduce == false)
				{
					for (int i = alreadyScannedPositions; i < min; i++)
					{
						if (i >= search.ScoreDocs.Length)
						{
							alreadyScannedPositions = i;
							var pageSizeIncreaseSize = min - search.ScoreDocs.Length;
							return pageSizeIncreaseSize;
						}
						var document = indexSearcher.Doc(search.ScoreDocs[i].Doc);
						var id = document.Get(Constants.DocumentIdFieldName);
						if (documentsAlreadySeenInPreviousPage.Add(id) == false)
						{
							// already seen this, need to expand the range we are scanning because the user
							// didn't take this into account
							min = Math.Min(min + 1, search.TotalHits);
							itemsSkipped++;
						}
					}
					alreadyScannedPositions = min;
				}


				if (fieldsToFetch.IsDistinctQuery)
				{
					// add results that were already there in previous pages
					for (int i = alreadyScannedPositionsForDistinct; i < min; i++)
					{
						if (i >= search.ScoreDocs.Length)
						{
							alreadyScannedPositionsForDistinct = i;
							var pageSizeIncreaseSize = min - search.ScoreDocs.Length;
							return pageSizeIncreaseSize;
						}

						Document document = indexSearcher.Doc(search.ScoreDocs[i].Doc);
						var indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, search.ScoreDocs[i]);
						if (indexQueryResult.Projection.Count > 0 && // we don't consider empty projections to be relevant for distinct operations
                            alreadyReturned.Add(indexQueryResult.Projection) == false)
						{
							min++; // we found a duplicate
							itemsSkipped++;
						}
					}
					alreadyScannedPositionsForDistinct = min;
				}
				if (adjustStart)
					start += itemsSkipped;
				return itemsSkipped;
			}
		}

		public IndexingPerformanceStats[] GetIndexingPerformance()
		{
			return currentlyIndexing.Values.Concat(indexingPerformanceStats).ToArray();
		}

		public IndexingPerformanceStats[] GetCurrentIndexingPerformance()
		{
			return currentlyIndexing.Values.ToArray();
		}

		public void Backup(string backupDirectory, string path, string incrementalTag)
		{
			if (directory is RAMDirectory)
				return; // nothing to backup in memory based index, will be reset on restore, anyway

			bool hasSnapshot = false;
			bool throwOnFinallyException = true;
			try
			{
				var existingFiles = new HashSet<string>();
				if (incrementalTag != null)
					backupDirectory = Path.Combine(backupDirectory, incrementalTag);

				var allFilesPath = Path.Combine(backupDirectory, indexId + ".all-existing-index-files");
				var saveToFolder = Path.Combine(backupDirectory, "Indexes", indexId.ToString());
				System.IO.Directory.CreateDirectory(saveToFolder);
				if (File.Exists(allFilesPath))
				{
					foreach (var file in File.ReadLines(allFilesPath))
					{
						existingFiles.Add(file);
					}
				}

				var neededFilePath = Path.Combine(saveToFolder, "index-files.required-for-index-restore");
				using (var allFilesWriter = File.Exists(allFilesPath) ? File.AppendText(allFilesPath) : File.CreateText(allFilesPath))
				using (var neededFilesWriter = File.CreateText(neededFilePath))
				{
					try
					{
						// this is called for the side effect of creating the snapshotter and the writer
						// we explicitly handle the backup outside of the write, to allow concurrent indexing
						Write((writer, analyzer, stats) =>
						{
							// however, we copy the current segments.gen & index.version to make 
							// sure that we get the _at the time_ of the write. 
							foreach (var fileName in new[] { "segments.gen", IndexStorage.IndexVersionFileName(indexDefinition)})
							{
								var fullPath = Path.Combine(path, indexId.ToString(), fileName);
								File.Copy(fullPath, Path.Combine(saveToFolder, fileName));
								allFilesWriter.WriteLine(fileName);
								neededFilesWriter.WriteLine(fileName);
							}
							return new IndexedItemsInfo();
						});
					}
					catch (CorruptIndexException e)
					{
						logIndexing.WarnException(
							"Could not backup index " + indexId +
							" because it is corrupted. Skipping the index, will force index reset on restore", e);
						neededFilesWriter.Dispose();
						TryDelete(neededFilePath);
						return;
					}

					var commit = snapshotter.Snapshot();
					hasSnapshot = true;
					foreach (var fileName in commit.FileNames)
					{
						var fullPath = Path.Combine(path, indexId.ToString(), fileName);

						if (".lock".Equals(Path.GetExtension(fullPath), StringComparison.InvariantCultureIgnoreCase))
							continue;

						if (File.Exists(fullPath) == false)
							continue;

						if (existingFiles.Contains(fileName) == false)
						{
							var destFileName = Path.Combine(saveToFolder, fileName);
							try
							{
								File.Copy(fullPath, destFileName);
							}
							catch (Exception e)
							{
								logIndexing.WarnException(
									"Could not backup index " + indexId +
									" because failed to copy file : " + fullPath + ". Skipping the index, will force index reset on restore", e);
								neededFilesWriter.Dispose();
								TryDelete(neededFilePath);
								return;

							}
							allFilesWriter.WriteLine(fileName);
						}
						neededFilesWriter.WriteLine(fileName);
					}
					allFilesWriter.Flush();
					neededFilesWriter.Flush();
				}
			}
			catch
			{
				throwOnFinallyException = false;
				throw;
			}
			finally
			{
				if (snapshotter != null && hasSnapshot)
				{
					try
					{
						snapshotter.Release();
					}
					catch
					{
						if (throwOnFinallyException)
							throw;
					}
				}
			}
		}

		private static void TryDelete(string neededFilePath)
		{
			try
			{
				File.Delete(neededFilePath);
			}
			catch (Exception)
			{
			}
		}

		protected object LoadDocument(string key)
		{
			var jsonDocument = context.Database.Documents.Get(key, null);
			if (jsonDocument == null)
				return new DynamicNullObject();
			return new DynamicJsonObject(jsonDocument.ToJson());
		}

        protected void UpdateDocumentReferences(IStorageActionsAccessor actions, 
            ConcurrentQueue<IDictionary<string, HashSet<string>>> allReferencedDocs,
			ConcurrentQueue<IDictionary<string, HashSet<string>>> missingReferencedDocs)
        {
            IDictionary<string, HashSet<string>> result;
            while (allReferencedDocs.TryDequeue(out result))
            {
                foreach (var referencedDocument in result)
                {
                    actions.Indexing.UpdateDocumentReferences(IndexId, referencedDocument.Key, referencedDocument.Value);
                    actions.General.MaybePulseTransaction();
                }
            }
            var task = new TouchMissingReferenceDocumentTask
            {
				Index = IndexId, // so we will get IsStale properly
                MissingReferences = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase)
            };

			var set = context.DoNotTouchAgainIfMissingReferences.GetOrAdd(IndexId, _ => new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase));
			IDictionary<string, HashSet<string>> docs;
            while (missingReferencedDocs.TryDequeue(out docs))
            {
                foreach (var doc in docs)
                {
                    if (set.TryRemove(doc.Key))
                        continue;
                    task.MissingReferences.Add(doc);
                }
            }
            if (task.MissingReferences.Count == 0)
                return;
            actions.Tasks.AddTask(task, SystemTime.UtcNow);
        }

                  
		public void ForceWriteToDisk()
		{
			forceWriteToDisk = true;
		}

        protected void EnsureValidNumberOfOutputsForDocument(string sourceDocumentId, int numberOfAlreadyProducedOutputs)
        {
            var maxNumberOfIndexOutputs = indexDefinition.MaxIndexOutputsPerDocument ?? context.Configuration.MaxIndexOutputsPerDocument;

            if (maxNumberOfIndexOutputs == -1)
                return;

            if (numberOfAlreadyProducedOutputs <= maxNumberOfIndexOutputs) 
                return;

            Priority = IndexingPriority.Error;

            // this cannot happen in the current transaction, since we are going to throw in just a bit.
            using (context.Database.TransactionalStorage.DisableBatchNesting())
            {
                context.Database.TransactionalStorage.Batch(accessor =>
                {
                    accessor.Indexing.SetIndexPriority(indexId, IndexingPriority.Error);
                    accessor.Indexing.TouchIndexEtag(indexId);
                });    
            }

            context.Database.Notifications.RaiseNotifications(new IndexChangeNotification()
            {
                Name = PublicName,
                Type = IndexChangeTypes.IndexMarkedAsErrored 
            });

            throw new InvalidOperationException(
                string.Format(
                    "Index '{0}' has already produced {1} map results for a source document '{2}', while the allowed max number of outputs is {3} per one document. " +
                    "Index will be disabled.  Please verify this index definition and consider a re-design of your entities.",
					PublicName, numberOfAlreadyProducedOutputs, sourceDocumentId, maxNumberOfIndexOutputs));
        }

		internal class IndexByIdEqualityComparer : IEqualityComparer<Index>
		{
			public bool Equals(Index x, Index y)
			{
				return x.IndexId == y.IndexId;
			}

			public int GetHashCode(Index obj)
			{
				return obj.IndexId.GetHashCode();
			}
		}
	}
}
