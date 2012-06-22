//-----------------------------------------------------------------------
// <copyright file="Index.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Spatial4n.Core.Query;
using Version = Lucene.Net.Util.Version;

namespace Raven.Database.Indexing
{
	/// <summary>
	/// 	This is a thread safe, single instance for a particular index.
	/// </summary>
	public abstract class Index : IDisposable
	{
		protected static readonly Logger logIndexing = LogManager.GetLogger(typeof(Index).FullName + ".Indexing");
		protected static readonly Logger logQuerying = LogManager.GetLogger(typeof(Index).FullName + ".Querying");
		private readonly List<Document> currentlyIndexDocuments = new List<Document>();
		private Directory directory;
		protected readonly IndexDefinition indexDefinition;
		private int docCountSinceLastOptimization;

		private readonly ConcurrentDictionary<string, IIndexExtension> indexExtensions =
			new ConcurrentDictionary<string, IIndexExtension>();

		protected readonly string name;

		private readonly AbstractViewGenerator viewGenerator;
		private readonly InMemoryRavenConfiguration configuration;
		private readonly object writeLock = new object();
		private volatile bool disposed;
		private IndexWriter indexWriter;
		private readonly IndexSearcherHolder currentIndexSearcherHolder = new IndexSearcherHolder();


		protected Index(Directory directory, string name, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator, InMemoryRavenConfiguration configuration)
		{
			if (directory == null) throw new ArgumentNullException("directory");
			if (name == null) throw new ArgumentNullException("name");
			if (indexDefinition == null) throw new ArgumentNullException("indexDefinition");
			if (viewGenerator == null) throw new ArgumentNullException("viewGenerator");

			this.name = name;
			this.indexDefinition = indexDefinition;
			this.viewGenerator = viewGenerator;
			this.configuration = configuration;
			logIndexing.Debug("Creating index for {0}", name);
			this.directory = directory;

			RecreateSearcher();
		}

		[ImportMany]
		public OrderedPartCollection<AbstractAnalyzerGenerator> AnalyzerGenerators { get; set; }

		/// <summary>
		/// Whatever this is a map reduce index or not
		/// </summary>
		public abstract bool IsMapReduce { get; }

		

		public void Dispose()
		{
			lock (writeLock)
			{
				disposed = true;
				foreach (var indexExtension in indexExtensions)
				{
					indexExtension.Value.Dispose();
				}
				if (currentIndexSearcherHolder != null)
				{
					currentIndexSearcherHolder.SetIndexSearcher(null);
				}

				if (indexWriter != null)
				{
					var writer = indexWriter;
					indexWriter = null;

					try
					{
						writer.GetAnalyzer().Close();
					}
					catch (Exception e)
					{
						logIndexing.ErrorException("Error while closing the index (closing the analyzer failed)", e);
					}

					try
					{
						writer.Close();
					}
					catch (Exception e)
					{
						logIndexing.ErrorException("Error when closing the index", e);
					}
				}

				try
				{
					directory.Close();
				}
				catch (Exception e)
				{
					logIndexing.ErrorException("Error when closing the directory", e);
				}
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

				indexWriter.Commit();
			}
		}

		public void MergeSegments()
		{
			if (docCountSinceLastOptimization <= 2048) return;
			lock (writeLock)
			{
				indexWriter.Optimize();
				docCountSinceLastOptimization = 0;
			}
		}

		public abstract void IndexDocuments(AbstractViewGenerator viewGenerator, IEnumerable<object> documents,
											WorkContext context, IStorageActionsAccessor actions, DateTime minimumTimestamp);


		protected virtual IndexQueryResult RetrieveDocument(Document document, FieldsToFetch fieldsToFetch, float score)
		{
			return new IndexQueryResult
			{
				Score = score,
				Key = document.Get(Constants.DocumentIdFieldName),
				Projection = fieldsToFetch.IsProjection ? CreateDocumentFromFields(document, fieldsToFetch) : null
			};
		}

		public static RavenJObject CreateDocumentFromFields(Document document, FieldsToFetch fieldsToFetch)
		{
			var documentFromFields = new RavenJObject();
			IEnumerable<string> fields = fieldsToFetch.Fields;

			if (fieldsToFetch.FetchAllStoredFields)
				fields = fields.Concat(document.GetFields().Cast<Fieldable>().Select(x => x.Name()));

			var q = fields
				.SelectMany(name => document.GetFields(name) ?? new Field[0])
				.Where(x => x != null)
				.Where(
					x =>
					x.Name().EndsWith("_IsArray") == false &&
					x.Name().EndsWith("_Range") == false &&
					x.Name().EndsWith("_ConvertToJson") == false)
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

		private static KeyValuePair<string, RavenJToken> CreateProperty(Field fld, Document document)
		{
			if (fld.IsBinary())
				return new KeyValuePair<string, RavenJToken>(fld.Name(), fld.GetBinaryValue());
			var stringValue = fld.StringValue();
			if (document.GetField(fld.Name() + "_ConvertToJson") != null)
			{
				var val = RavenJToken.Parse(fld.StringValue()) as RavenJObject;
				return new KeyValuePair<string, RavenJToken>(fld.Name(), val);
			}
			if (stringValue == Constants.NullValue)
				stringValue = null;
			if (stringValue == Constants.EmptyString)
				stringValue = string.Empty;
			return new KeyValuePair<string, RavenJToken>(fld.Name(), stringValue);
		}

		protected void Write(WorkContext context, Func<IndexWriter, Analyzer, IndexingWorkStats, int> action)
		{
			if (disposed)
				throw new ObjectDisposedException("Index " + name + " has been disposed");
			lock (writeLock)
			{
				bool shouldRecreateSearcher;
				var toDispose = new List<Action>();
				Analyzer searchAnalyzer = null;
				try
				{
					try
					{
						searchAnalyzer = CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose);
					}
					catch (Exception e)
					{
						context.AddError(name, "Creating Analyzer", e.ToString());
						throw;
					}

					if (indexWriter == null)
					{
						indexWriter = CreateIndexWriter(directory);
					}

					var stats = new IndexingWorkStats();
					try
					{
						var changedDocs = action(indexWriter, searchAnalyzer, stats);
						docCountSinceLastOptimization += changedDocs;
						shouldRecreateSearcher = changedDocs > 0;
						foreach (IIndexExtension indexExtension in indexExtensions.Values)
						{
							indexExtension.OnDocumentsIndexed(currentlyIndexDocuments);
						}
					}
					catch (Exception e)
					{
						context.AddError(name, null, e.ToString());
						throw;
					}

					UpdateIndexingStats(context, stats);

					WriteTempIndexToDiskIfNeeded(context);

					if (configuration.TransactionMode == TransactionMode.Safe)
						Flush(); // just make sure changes are flushed to disk
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
				}
				if (shouldRecreateSearcher)
					RecreateSearcher();
			}
		}

		protected void UpdateIndexingStats(WorkContext context, IndexingWorkStats stats)
		{
			context.TransactionaStorage.Batch(accessor =>
			{
				switch (stats.Operation)
				{
					case IndexingWorkStats.Status.Map:
						accessor.Indexing.UpdateIndexingStats(name, stats);
						break;
					case IndexingWorkStats.Status.Reduce:
						accessor.Indexing.UpdateReduceStats(name, stats);
						break;
					case IndexingWorkStats.Status.Ignore:
						break;
					default:
						throw new ArgumentOutOfRangeException();
				}
			});
		}

		private static IndexWriter CreateIndexWriter(Directory directory)
		{
			var indexWriter = new IndexWriter(directory, new StopAnalyzer(Version.LUCENE_29), IndexWriter.MaxFieldLength.UNLIMITED);
			var mergeScheduler = indexWriter.GetMergeScheduler();
			if (mergeScheduler != null)
				mergeScheduler.Close();
			indexWriter.SetMergeScheduler(new ErrorLoggingConcurrentMergeScheduler());
			return indexWriter;
		}

		private void WriteTempIndexToDiskIfNeeded(WorkContext context)
		{
			if (context.Configuration.RunInMemory || !indexDefinition.IsTemp)
				return;

			var dir = indexWriter.GetDirectory() as RAMDirectory;
			if (dir == null ||
				dir.SizeInBytes() < context.Configuration.TempIndexInMemoryMaxBytes)
				return;

			indexWriter.Commit();
			var fsDir = context.IndexStorage.MakeRAMDirectoryPhysical(dir, indexDefinition.Name);
			directory = fsDir;

			indexWriter.GetAnalyzer().Close();
			indexWriter.Close();

			indexWriter = CreateIndexWriter(directory);
		}

		public PerFieldAnalyzerWrapper CreateAnalyzer(Analyzer defaultAnalyzer, ICollection<Action> toDispose, bool forQuerying = false)
		{
			toDispose.Add(defaultAnalyzer.Close);

			string value;
			if(indexDefinition.Analyzers.TryGetValue(Constants.AllFields, out value))
			{
				defaultAnalyzer = IndexingExtensions.CreateAnalyzerInstance(Constants.AllFields, value);
				toDispose.Add(defaultAnalyzer.Close);
			}
			var perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(defaultAnalyzer);
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

		protected IEnumerable<object> RobustEnumerationIndex(IEnumerable<object> input, IEnumerable<IndexingFunc> funcs,
															IStorageActionsAccessor actions, WorkContext context, IndexingWorkStats stats)
		{
			return new RobustEnumerator(context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
			{
				BeforeMoveNext = () => stats.IndexingAttempts++,
				CancelMoveNext = () => stats.IndexingAttempts--,
				OnError = (exception, o) =>
				{
					context.AddError(name,
									TryGetDocKey(o),
									exception.Message
						);
					logIndexing.WarnException(
						String.Format("Failed to execute indexing function on {0} on {1}", name,
										TryGetDocKey(o)),
						exception);

					stats.IndexingErrors++;
				}
			}.RobustEnumeration(input, funcs);
		}

		protected IEnumerable<object> RobustEnumerationReduce(IEnumerable<object> input, IndexingFunc func,
															IStorageActionsAccessor actions, WorkContext context,
			IndexingWorkStats stats)
		{
			// not strictly accurate, but if we get that many errors, probably an error anyway.
			return new RobustEnumerator(context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
			{
				BeforeMoveNext = () => stats.ReduceAttempts++,
				CancelMoveNext = () => stats.ReduceAttempts--,
				OnError = (exception, o) =>
				{
					context.AddError(name,
									TryGetDocKey(o),
									exception.Message
						);
					logIndexing.WarnException(
						String.Format("Failed to execute indexing function on {0} on {1}", name,
										TryGetDocKey(o)),
						exception);

					stats.ReduceErrors++;
				}
			}.RobustEnumeration(input, func);
		}

		// we don't care about tracking map/reduce stats here, since it is merely
		// an optimization step
		protected IEnumerable<object> RobustEnumerationReduceDuringMapPhase(IEnumerable<object> input, IndexingFunc func,
															IStorageActionsAccessor actions, WorkContext context)
		{
			// not strictly accurate, but if we get that many errors, probably an error anyway.
			return new RobustEnumerator(context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
			{
				BeforeMoveNext = () => { }, // don't care
				CancelMoveNext = () => { }, // don't care
				OnError = (exception, o) =>
				{
					context.AddError(name,
									TryGetDocKey(o),
									exception.Message
						);
					logIndexing.WarnException(
						String.Format("Failed to execute indexing function on {0} on {1}", name,
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

		internal IDisposable GetSearcher(out IndexSearcher searcher)
		{
			return currentIndexSearcherHolder.GetSearcher(out searcher);
		}

		private void RecreateSearcher()
		{
			if (indexWriter == null)
			{
				currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(directory, true));
			}
			else
			{
				var indexReader = indexWriter.GetReader();
				currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(indexReader));
			}
		}

		protected void AddDocumentToIndex(IndexWriter currentIndexWriter, Document luceneDoc, Analyzer analyzer)
		{
			Analyzer newAnalyzer = AnalyzerGenerators.Aggregate(analyzer,
																(currentAnalyzer, generator) =>
																{
																	Analyzer generateAnalyzer =
																		generator.Value.GenerateAnalyzerForIndexing(name, luceneDoc,
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
			}
			finally
			{
				if (newAnalyzer != analyzer)
					newAnalyzer.Close();
			}
		}

		public IIndexExtension GetExtension(string indexExtensionKey)
		{
			IIndexExtension val;
			indexExtensions.TryGetValue(indexExtensionKey, out val);
			return val;
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
					var clonedNumericField = new NumericField(numericField.Name(),
															numericField.IsStored() ? Field.Store.YES : Field.Store.NO,
															numericField.IsIndexed());
					var numericValue = numericField.GetNumericValue();
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
					if (field.IsBinary())
					{
						clonedField = new Field(field.Name(), field.BinaryValue(),
												field.IsStored() ? Field.Store.YES : Field.Store.NO);
					}
					else
					{
						clonedField = new Field(field.Name(), field.StringValue(),
										field.IsStored() ? Field.Store.YES : Field.Store.NO,
										field.IsIndexed() ? Field.Index.ANALYZED_NO_NORMS : Field.Index.NOT_ANALYZED_NO_NORMS);
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
				var fieldsForLogging = luceneDoc.GetFields().Cast<Fieldable>().Select(x => new
				{
					Name = x.Name(),
					Value = x.IsBinary() ? "<binary>" : x.StringValue(),
					Indexed = x.IsIndexed(),
					Stored = x.IsStored(),
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

				logIndexing.Debug("Indexing on {0} result in index {1} gave document: {2}", key, name,
								sb.ToString());
			}
		}


		#region Nested type: IndexQueryOperation

		internal class IndexQueryOperation
		{
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

			public IEnumerable<IndexQueryResult> Query()
			{
				using (IndexStorage.EnsureInvariantCulture())
				{
					AssertQueryDoesNotContainFieldsThatAreNotIndexes();
					IndexSearcher indexSearcher;
					using (parent.GetSearcher(out indexSearcher))
					{
						var luceneQuery = ApplyIndexTriggers(GetLuceneQuery());

						int start = indexQuery.Start;
						int pageSize = indexQuery.PageSize;
						int returnedResults = 0;
						int skippedResultsInCurrentLoop = 0;
						do
						{
							if (skippedResultsInCurrentLoop > 0)
							{
								start = start + pageSize;
								// trying to guesstimate how many results we will need to read from the index
								// to get enough unique documents to match the page size
								pageSize = skippedResultsInCurrentLoop * indexQuery.PageSize;
								skippedResultsInCurrentLoop = 0;
							}
							TopDocs search = ExecuteQuery(indexSearcher, luceneQuery, start, pageSize, indexQuery);
							indexQuery.TotalSize.Value = search.TotalHits;

							RecordResultsAlreadySeenForDistinctQuery(indexSearcher, search, start, pageSize);
							
							for (var i = start; (i - start) < pageSize && i < search.ScoreDocs.Length; i++)
							{
								Document document = indexSearcher.Doc(search.ScoreDocs[i].doc);
								IndexQueryResult indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, search.ScoreDocs[i].score);
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
						} while (skippedResultsInCurrentLoop > 0 && returnedResults < indexQuery.PageSize);
					}
				}
			}

			private Query ApplyIndexTriggers(Query luceneQuery)
			{
				luceneQuery = indexQueryTriggers.Aggregate(luceneQuery,
				                                           (current, indexQueryTrigger) =>
				                                           indexQueryTrigger.Value.ProcessQuery(parent.name, current, indexQuery));
				return luceneQuery;
			}

			public IEnumerable<IndexQueryResult> IntersectionQuery()
			{
				using (IndexStorage.EnsureInvariantCulture())
				{
					AssertQueryDoesNotContainFieldsThatAreNotIndexes();
					IndexSearcher indexSearcher;
					using (parent.GetSearcher(out indexSearcher))
					{
						var subQueries = indexQuery.Query.Split(new[] { Constants.IntersectSeperator }, StringSplitOptions.RemoveEmptyEntries);
						if (subQueries.Length <= 1)
							throw new InvalidOperationException("Invalid INTRESECT query, must have multiple intersect clauses.");

						//Not sure how to select the page size here??? The problem is that only docs in this search can be part 
						//of the final result because we're doing an intersection query (but we might exclude some of them)
						int pageSizeBestGuess = (indexQuery.Start + indexQuery.PageSize) * 2;
						int intersectMatches = 0, skippedResultsInCurrentLoop = 0;
						int previousBaseQueryMatches = 0, currentBaseQueryMatches = 0;

						var firstSubLuceneQuery = ApplyIndexTriggers(GetLuceneQuery(subQueries[0], indexQuery.DefaultField));

						//Do the first sub-query in the normal way, so that sorting, filtering etc is accounted for
						var search = ExecuteQuery(indexSearcher, firstSubLuceneQuery, 0, pageSizeBestGuess, indexQuery);
						currentBaseQueryMatches = search.ScoreDocs.Length;
						var intersectionCollector = new IntersectionCollector(indexSearcher, search.ScoreDocs);

						do
						{
							if (skippedResultsInCurrentLoop > 0)
							{
								// We get here because out first attempt didn't get enough docs (after INTERSECTION was calculated)
								pageSizeBestGuess = pageSizeBestGuess * 2;

								search = ExecuteQuery(indexSearcher, firstSubLuceneQuery, 0, pageSizeBestGuess, indexQuery);
								previousBaseQueryMatches = currentBaseQueryMatches;
								currentBaseQueryMatches = search.ScoreDocs.Length;
								intersectionCollector = new IntersectionCollector(indexSearcher, search.ScoreDocs);
							}

							for (int i = 1; i < subQueries.Length; i++)
							{
								var luceneSubQuery = ApplyIndexTriggers(GetLuceneQuery(subQueries[i], indexQuery.DefaultField));
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
							IndexQueryResult indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, search.ScoreDocs[i].score);
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
						var document = indexSearcher.Doc(search.ScoreDocs[i].doc);
						documentsAlreadySeenInPreviousPage.Add(document.Get(Constants.DocumentIdFieldName));
					}
				}

				if (fieldsToFetch.IsDistinctQuery == false)
					return;

				// add results that were already there in previous pages
				for (int i = 0; i < min; i++)
				{
					Document document = indexSearcher.Doc(search.ScoreDocs[i].doc);
					var indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, search.ScoreDocs[i].score);
					alreadyReturned.Add(indexQueryResult.Projection);
				}
			}

			private void AssertQueryDoesNotContainFieldsThatAreNotIndexes()
			{
				HashSet<string> hashSet = SimpleQueryParser.GetFields(indexQuery);
				foreach (string field in hashSet)
				{
					string f = field;
					if (f.EndsWith("_Range"))
					{
						f = f.Substring(0, f.Length - "_Range".Length);
					}
					if (parent.viewGenerator.ContainsField(f) == false &&
						parent.viewGenerator.ContainsField("_") == false) // the catch all field name means that we have dynamic fields names
						throw new ArgumentException("The field '" + f + "' is not indexed, cannot query on fields that are not indexed");
				}

				if (indexQuery.SortedFields == null)
					return;

				foreach (SortedField field in indexQuery.SortedFields)
				{
					string f = field.Field;
					if (f.EndsWith("_Range"))
					{
						f = f.Substring(0, f.Length - "_Range".Length);
					}
					if (f.StartsWith(Constants.RandomFieldName))
						continue;
					if (parent.viewGenerator.ContainsField(f) == false && f != Constants.DistanceFieldName
						&& parent.viewGenerator.ContainsField("_") == false)// the catch all field name means that we have dynamic fields names
						throw new ArgumentException("The field '" + f + "' is not indexed, cannot sort on fields that are not indexed");
				}
			}

			public Query GetLuceneQuery()
			{
				var q = GetLuceneQuery(indexQuery.Query, indexQuery.DefaultField);
				var spatialIndexQuery = indexQuery as SpatialIndexQuery;
				if (spatialIndexQuery != null)
				{
					var dq = SpatialIndex.MakeQuery(spatialIndexQuery.Latitude, spatialIndexQuery.Longitude, spatialIndexQuery.Radius);
					if (q is MatchAllDocsQuery) return dq;

					var bq = new BooleanQuery();
					bq.Add(q, BooleanClause.Occur.MUST);
					bq.Add(dq, BooleanClause.Occur.MUST);
					return bq;
				}
				return q;
			}

			private Query GetLuceneQuery(string query, string defaultField)
			{				
				Query luceneQuery;
				if (String.IsNullOrEmpty(query))
				{
					logQuerying.Debug("Issuing query on index {0} for all documents", parent.name);
					luceneQuery = new MatchAllDocsQuery();
				}
				else
				{
					logQuerying.Debug("Issuing query on index {0} for: {1}", parent.name, query);
					var toDispose = new List<Action>();
					PerFieldAnalyzerWrapper searchAnalyzer = null;
					try
					{
						searchAnalyzer = parent.CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose, true);
						searchAnalyzer = parent.AnalyzerGenerators.Aggregate(searchAnalyzer, (currentAnalyzer, generator) =>
						{
							Analyzer newAnalyzer = generator.GenerateAnalzyerForQuerying(parent.name, indexQuery.Query, currentAnalyzer);
							if (newAnalyzer != currentAnalyzer)
							{
								DisposeAnalyzerAndFriends(toDispose, currentAnalyzer);
							}
							return parent.CreateAnalyzer(newAnalyzer, toDispose, true);
						});
						luceneQuery = QueryBuilder.BuildQuery(query, defaultField, searchAnalyzer);
					}
					finally
					{
						DisposeAnalyzerAndFriends(toDispose, searchAnalyzer);
					}
				}
				return luceneQuery;
			}

			private static void DisposeAnalyzerAndFriends(List<Action> toDispose, PerFieldAnalyzerWrapper analyzer)
			{
				if (analyzer != null)
					analyzer.Close();
				foreach (Action dispose in toDispose)
				{
					dispose();
				}
				toDispose.Clear();
			}

			private TopDocs ExecuteQuery(IndexSearcher indexSearcher, Query luceneQuery, int start, int pageSize,
										IndexQuery indexQuery)
			{
				var sort = indexQuery.GetSort(parent.indexDefinition);

				if (pageSize == Int32.MaxValue) // we want all docs
				{
					var gatherAllCollector = new GatherAllCollector();
					indexSearcher.Search(luceneQuery, gatherAllCollector);
					return gatherAllCollector.ToTopDocs();
				}
				var minPageSize = Math.Max(pageSize + start, 1);

				// NOTE: We get Start + Pagesize results back so we have something to page on
				if (sort != null)
				{
					var ret = indexSearcher.Search(luceneQuery, null, minPageSize, sort);
					return ret;
				}
				return indexSearcher.Search(luceneQuery, null, minPageSize);
			}
		}

		#endregion
	}
}