//-----------------------------------------------------------------------
// <copyright file="Index.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Threading;
using log4net;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Version = Lucene.Net.Util.Version;

namespace Raven.Database.Indexing
{
	/// <summary>
	/// 	This is a thread safe, single instance for a particular index.
	/// </summary>
	public abstract class Index : IDisposable
	{
		protected static readonly ILog logIndexing = LogManager.GetLogger(typeof (Index) + ".Indexing");
		protected static readonly ILog logQuerying = LogManager.GetLogger(typeof (Index) + ".Querying");
		private readonly List<Document> currentlyIndexDocumented = new List<Document>();
		private readonly Directory directory;
		protected readonly IndexDefinition indexDefinition;

		private readonly ConcurrentDictionary<string, IIndexExtension> indexExtensions =
			new ConcurrentDictionary<string, IIndexExtension>();

		protected readonly string name;

		private readonly AbstractViewGenerator viewGenerator;
		private readonly object writeLock = new object();
		private volatile bool disposed;
		private IndexWriter indexWriter;
		private CurrentIndexSearcher searcher;


		protected Index(Directory directory, string name, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator)
		{
			if (directory == null) throw new ArgumentNullException("directory");
			if (name == null) throw new ArgumentNullException("name");
			if (indexDefinition == null) throw new ArgumentNullException("indexDefinition");
			if (viewGenerator == null) throw new ArgumentNullException("viewGenerator");

			this.name = name;
			this.indexDefinition = indexDefinition;
			this.viewGenerator = viewGenerator;
			logIndexing.DebugFormat("Creating index for {0}", name);
			this.directory = directory;

			// clear any locks that are currently held
			// this may happen if the server crashed while
			// writing to the index
			this.directory.ClearLock("write.lock");

			searcher = new CurrentIndexSearcher(new IndexSearcher(directory, true));
		}

		[ImportMany]
		public IEnumerable<AbstractAnalyzerGenerator> AnalyzerGenerators { get; set; }

		internal CurrentIndexSearcher Searcher
		{
			get { return searcher; }
		}

		#region IDisposable Members

		public void Dispose()
		{
			lock (writeLock)
			{
				disposed = true;
				foreach (var indexExtension in indexExtensions)
				{
					indexExtension.Value.Dispose();
				}
				IndexSearcher _;
				using (searcher.Use(out _))
					searcher.MarkForDispoal();
				if (indexWriter != null)
				{
					IndexWriter writer = indexWriter;
					indexWriter = null;
					writer.Close();
				}
				directory.Close();
			}
		}

		#endregion

		public void Flush()
		{
			lock (writeLock)
			{
				if (disposed)
					return;
				if (indexWriter != null)
				{
					indexWriter.Commit();
				}
			}
		}

		public abstract void IndexDocuments(AbstractViewGenerator viewGenerator, IEnumerable<object> documents,
		                                    WorkContext context, IStorageActionsAccessor actions, DateTime minimumTimestamp);


		protected virtual IndexQueryResult RetrieveDocument(Document document, string[] fieldsToFetch)
		{
			bool shouldBuildProjection = fieldsToFetch == null || fieldsToFetch.Length == 0;
			return new IndexQueryResult
			{
				Key = document.Get("__document_id"),
				Projection = shouldBuildProjection ? null : CreateDocumentFromFields(document, fieldsToFetch)
			};
		}

		private static JObject CreateDocumentFromFields(Document document, IEnumerable<string> fieldsToFetch)
		{
			return new JObject(
				fieldsToFetch.Concat(new[] {"__document_id"}).Distinct()
					.SelectMany(name => document.GetFields(name) ?? new Field[0])
					.Where(x => x != null)
					.Where(
						x =>
						x.Name().EndsWith("_IsArray") == false && x.Name().EndsWith("_Range") == false &&
						x.Name().EndsWith("_ConvertToJson") == false)
					.Select(fld => CreateProperty(fld, document))
					.GroupBy(x => x.Name)
					.Select(g =>
					{
						if (g.Count() == 1 && document.GetField(g.Key + "_IsArray") == null)
						{
							return g.First();
						}
						return new JProperty(g.Key, g.Select(x => x.Value));
					})
				);
		}

		private static JProperty CreateProperty(Field fld, Document document)
		{
			if (document.GetField(fld.Name() + "_ConvertToJson") != null)
			{
				object val = JsonConvert.DeserializeObject(fld.StringValue());
				return new JProperty(fld.Name(), val);
			}
			return new JProperty(fld.Name(), fld.StringValue());
		}

		protected void Write(WorkContext context, Func<IndexWriter, Analyzer, bool> action)
		{
			if (disposed)
				throw new ObjectDisposedException("Index " + name + " has been disposed");
			lock (writeLock)
			{
				bool shouldRecreateSearcher;
				var toDispose = new List<Action>();
				Analyzer analyzer = null;
				try
				{
					try
					{
						analyzer = CreateAnalyzer(new LowerCaseAnalyzer(), toDispose);
					}
					catch (Exception e)
					{
						context.AddError(name, "Creating Analyzer", e.ToString());
						throw;
					}
					if (indexWriter == null)
						indexWriter = new IndexWriter(directory, new StopAnalyzer(Version.LUCENE_29), IndexWriter.MaxFieldLength.UNLIMITED);
					try
					{
						shouldRecreateSearcher = action(indexWriter, analyzer);
						foreach (IIndexExtension indexExtension in indexExtensions.Values)
						{
							indexExtension.OnDocumentsIndexed(currentlyIndexDocumented);
						}
					}
					catch (Exception e)
					{
						context.AddError(name, null, e.ToString());
						throw;
					}
				}
				finally
				{
					currentlyIndexDocumented.Clear();
					if (analyzer != null)
						analyzer.Close();
					foreach (Action dispose in toDispose)
					{
						dispose();
					}
				}
				if (shouldRecreateSearcher)
					RecreateSearcher();
			}
		}

		public PerFieldAnalyzerWrapper CreateAnalyzer(Analyzer defaultAnalyzer, ICollection<Action> toDispose)
		{
			toDispose.Add(defaultAnalyzer.Close);
			var perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(defaultAnalyzer);
			foreach (var analyzer in indexDefinition.Analyzers)
			{
				Analyzer analyzerInstance = IndexingExtensions.CreateAnalyzerInstance(analyzer.Key, analyzer.Value);
				if (analyzerInstance == null)
					continue;
				toDispose.Add(analyzerInstance.Close);
				perFieldAnalyzerWrapper.AddAnalyzer(analyzer.Key, analyzerInstance);
			}
			StandardAnalyzer standardAnalyzer = null;
			KeywordAnalyzer keywordAnalyzer = null;
			foreach (var fieldIndexing in indexDefinition.Indexes)
			{
				switch (fieldIndexing.Value)
				{
					case FieldIndexing.NotAnalyzed:
					case FieldIndexing.NotAnalyzedNoNorms:
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

		protected IEnumerable<object> RobustEnumerationIndex(IEnumerable<object> input, IndexingFunc func,
		                                                     IStorageActionsAccessor actions, WorkContext context)
		{
			return new RobustEnumerator
			{
				BeforeMoveNext = actions.Indexing.IncrementIndexingAttempt,
				CancelMoveNext = actions.Indexing.DecrementIndexingAttempt,
				OnError = (exception, o) =>
				{
					context.AddError(name,
					                 TryGetDocKey(o),
					                 exception.Message
						);
					logIndexing.WarnFormat(exception, "Failed to execute indexing function on {0} on {1}", name,
					                       TryGetDocKey(o));
					try
					{
						actions.Indexing.IncrementIndexingFailure();
					}
					catch (Exception e)
					{
						// we don't care about error here, because it is an error on error problem
						logIndexing.WarnFormat(e, "Could not increment indexing failure rate for {0}", name);
					}
				}
			}.RobustEnumeration(input, func);
		}

		protected IEnumerable<object> RobustEnumerationReduce(IEnumerable<object> input, IndexingFunc func,
		                                                      IStorageActionsAccessor actions, WorkContext context)
		{
			return new RobustEnumerator
			{
				BeforeMoveNext = actions.Indexing.IncrementReduceIndexingAttempt,
				CancelMoveNext = actions.Indexing.DecrementReduceIndexingAttempt,
				OnError = (exception, o) =>
				{
					context.AddError(name,
					                 TryGetDocKey(o),
					                 exception.Message
						);
					logIndexing.WarnFormat(exception, "Failed to execute indexing function on {0} on {1}", name,
					                       TryGetDocKey(o));
					try
					{
						actions.Indexing.IncrementReduceIndexingFailure();
					}
					catch (Exception e)
					{
						// we don't care about error here, because it is an error on error problem
						logIndexing.WarnFormat(e, "Could not increment indexing failure rate for {0}", name);
					}
				}
			}.RobustEnumeration(input, func);
		}

		public static string TryGetDocKey(object current)
		{
			var dic = current as DynamicJsonObject;
			if (dic == null)
				return null;
			object value = dic.GetValue("__document_id");
			if (value == null)
				return null;
			return value.ToString();
		}

		public abstract void Remove(string[] keys, WorkContext context);


		private void RecreateSearcher()
		{
			IndexSearcher _;
			using (searcher.Use(out _))
			{
				searcher.MarkForDispoal();
				if (indexWriter == null)
				{
					searcher = new CurrentIndexSearcher(new IndexSearcher(directory, true));
				}
				else
				{
					searcher = new CurrentIndexSearcher(new IndexSearcher(indexWriter.GetReader()));
				}
				Thread.MemoryBarrier(); // force other threads to see this write
			}
		}

		protected void AddDocumentToIndex(IndexWriter currentIndexWriter, Document luceneDoc, Analyzer analyzer)
		{
			Analyzer newAnalyzer = AnalyzerGenerators.Aggregate(analyzer,
			                                                    (currentAnalyzer, generator) =>
			                                                    {
			                                                    	Analyzer generateAnalyzer =
			                                                    		generator.GenerateAnalyzerForIndexing(name, luceneDoc,
			                                                    		                                      currentAnalyzer);
			                                                    	if (generateAnalyzer != currentAnalyzer &&
			                                                    	    currentAnalyzer != analyzer)
			                                                    		currentAnalyzer.Close();
			                                                    	return generateAnalyzer;
			                                                    });

			try
			{
				if (indexExtensions.Count > 0)
					currentlyIndexDocumented.Add(luceneDoc);

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

		#region Nested type: CurrentIndexSearcher

		public class CurrentIndexSearcher
		{
			private readonly IndexSearcher searcher;
			private bool shouldDisposeWhenThereAreNoUsages;
			private int useCount;

			public CurrentIndexSearcher(IndexSearcher searcher)
			{
				this.searcher = searcher;
			}

			public IDisposable Use(out IndexSearcher indexSearcher)
			{
				Interlocked.Increment(ref useCount);
				indexSearcher = searcher;
				return new CleanUp(this);
			}

			public void MarkForDispoal()
			{
				shouldDisposeWhenThereAreNoUsages = true;
			}

			#region Nested type: CleanUp

			private class CleanUp : IDisposable
			{
				private readonly CurrentIndexSearcher parent;

				public CleanUp(CurrentIndexSearcher parent)
				{
					this.parent = parent;
				}

				#region IDisposable Members

				public void Dispose()
				{
					int uses = Interlocked.Decrement(ref parent.useCount);
					if (parent.shouldDisposeWhenThereAreNoUsages && uses == 0)
					{
						IndexReader indexReader = parent.searcher.GetIndexReader();
						parent.searcher.Close();
						indexReader.Close();
					}
				}

				#endregion
			}

			#endregion
		}

		#endregion

		#region Nested type: IndexQueryOperation

		internal class IndexQueryOperation
		{
			private readonly IndexQuery indexQuery;
			private readonly Index parent;
			private readonly Func<IndexQueryResult, bool> shouldIncludeInResults;

			public IndexQueryOperation(
				Index parent,
				IndexQuery indexQuery,
				Func<IndexQueryResult, bool> shouldIncludeInResults)
			{
				this.parent = parent;
				this.indexQuery = indexQuery;
				this.shouldIncludeInResults = shouldIncludeInResults;
			}

			public IEnumerable<IndexQueryResult> Query()
			{
				AssertQueryDoesNotContainFieldsThatAreNotIndexes();
				bool isDistinctQuery = indexQuery.AggregationOperation.HasFlag(AggregationOperation.Distinct);
				var alreadyReturned = new HashSet<IndexQueryResult>();
				IndexSearcher indexSearcher;
				using (parent.searcher.Use(out indexSearcher))
				{
					Query luceneQuery = GetLuceneQuery();
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
							pageSize = skippedResultsInCurrentLoop*indexQuery.PageSize;
							skippedResultsInCurrentLoop = 0;
						}
						TopDocs search = ExecuteQuery(indexSearcher, luceneQuery, start, pageSize, indexQuery);
						indexQuery.TotalSize.Value = search.totalHits;

						if (isDistinctQuery) // add results that were already there in previous pages
						{
							for (int i = 0; i < start; i++)
							{
								Document document = indexSearcher.Doc(search.scoreDocs[i].doc);
								alreadyReturned.Add(parent.RetrieveDocument(document, indexQuery.FieldsToFetch));
							}
						}

						for (int i = start; i < search.totalHits && (i - start) < pageSize; i++)
						{
							Document document = indexSearcher.Doc(search.scoreDocs[i].doc);
							IndexQueryResult indexQueryResult = parent.RetrieveDocument(document, indexQuery.FieldsToFetch);
							if (shouldIncludeInResults(indexQueryResult) == false)
							{
								indexQuery.SkippedResults.Value++;
								skippedResultsInCurrentLoop++;
								continue;
							}
							if (isDistinctQuery && alreadyReturned.Add(indexQueryResult))
								continue;
							returnedResults++;
							yield return indexQueryResult;
							if (returnedResults == indexQuery.PageSize)
								yield break;
						}
					} while (skippedResultsInCurrentLoop > 0 && returnedResults < indexQuery.PageSize);
				}
			}

			private void AssertQueryDoesNotContainFieldsThatAreNotIndexes()
			{
				HashSet<string> hashSet = SimpleQueryParser.GetFields(indexQuery.Query);
				foreach (string field in hashSet)
				{
					string f = field;
					if (f.EndsWith("_Range"))
					{
						f = f.Substring(0, f.Length - "_Range".Length);
					}
					if (parent.viewGenerator.ContainsField(f) == false)
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
					if (parent.viewGenerator.ContainsField(f) == false)
						throw new ArgumentException("The field '" + f + "' is not indexed, cannot sort on fields that are not indexed");
				}
			}

			private Query GetLuceneQuery()
			{
				string query = indexQuery.Query;
				Query luceneQuery;
				if (string.IsNullOrEmpty(query))
				{
					logQuerying.DebugFormat("Issuing query on index {0} for all documents", parent.name);
					luceneQuery = new MatchAllDocsQuery();
				}
				else
				{
					logQuerying.DebugFormat("Issuing query on index {0} for: {1}", parent.name, query);
					var toDispose = new List<Action>();
					PerFieldAnalyzerWrapper analyzer = null;
					try
					{
						analyzer = parent.CreateAnalyzer(new LowerCaseAnalyzer(), toDispose);
						analyzer = parent.AnalyzerGenerators.Aggregate(analyzer, (currentAnalyzer, generator) =>
						{
							Analyzer newAnalyzer = generator.GenerateAnalzyerForQuerying(parent.name, indexQuery.Query, currentAnalyzer);
							if (newAnalyzer != currentAnalyzer)
							{
								DisposeAnalyzerAndFriends(toDispose, currentAnalyzer);
							}
							return parent.CreateAnalyzer(newAnalyzer, toDispose);
							;
						});
						luceneQuery = QueryBuilder.BuildQuery(query, analyzer);
					}
					finally
					{
						DisposeAnalyzerAndFriends(toDispose, analyzer);
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
				Filter filter = indexQuery.GetFilter();
				Sort sort = indexQuery.GetSort(filter, parent.indexDefinition);

				if (pageSize == int.MaxValue) // we want all docs
				{
					var gatherAllCollector = new GatherAllCollector();
					indexSearcher.Search(luceneQuery, filter, gatherAllCollector);
					return gatherAllCollector.ToTopDocs();
				}
				// NOTE: We get Start + Pagesize results back so we have something to page on
				if (sort != null)
				{
					return indexSearcher.Search(luceneQuery, filter, pageSize + start, sort);
				}
				return indexSearcher.Search(luceneQuery, filter, pageSize + start);
			}
		}

		#endregion
	}
}