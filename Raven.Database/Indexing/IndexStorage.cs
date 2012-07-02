//-----------------------------------------------------------------------
// <copyright file="IndexStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Queries;
using Raven.Database.Storage;
using Directory = System.IO.Directory;
using System.ComponentModel.Composition;

namespace Raven.Database.Indexing
{
	/// <summary>
	/// 	Thread safe, single instance for the entire application
	/// </summary>
	public class IndexStorage : CriticalFinalizerObject, IDisposable
	{
		private const string IndexVersion = "1.2.17";

		private readonly IndexDefinitionStorage indexDefinitionStorage;
		private readonly InMemoryRavenConfiguration configuration;
		private readonly string path;
		private readonly ConcurrentDictionary<string, Index> indexes = new ConcurrentDictionary<string, Index>(StringComparer.InvariantCultureIgnoreCase);
		private static readonly Logger log = LogManager.GetCurrentClassLogger();
		private static readonly Logger startupLog = LogManager.GetLogger(typeof (IndexStorage).FullName + ".Startup");
		private readonly Analyzer dummyAnalyzer = new SimpleAnalyzer();

		private readonly FileStream crashMarker;

		public IndexStorage(IndexDefinitionStorage indexDefinitionStorage, InMemoryRavenConfiguration configuration, DocumentDatabase documentDatabase)
		{
			this.indexDefinitionStorage = indexDefinitionStorage;
			this.configuration = configuration;
			path = configuration.IndexStoragePath;

			if (Directory.Exists(path) == false && configuration.RunInMemory == false)
				Directory.CreateDirectory(path);


			if (configuration.RunInMemory == false)
			{
				var crashMarkerPath = Path.Combine(path, "indexing.crash-marker");

				if (File.Exists(crashMarkerPath))
				{
					// the only way this can happen is if we crashed because of a power outage
					// in this case, we consider all open indexes to be corrupt and force them
					// to be reset. This is because to get better perf, we don't flush the files to disk,
					// so in the case of a power outage, we can't be sure that there wasn't still stuff in
					// the OS buffer that wasn't written yet.
					configuration.ResetIndexOnUncleanShutdown = true;
				}

				// The delete on close ensures that the only way this file will exists is if there was
				// a power outage while the server was running.
				crashMarker = File.Create(crashMarkerPath, 16, FileOptions.DeleteOnClose);
			}

			foreach (var indexName in indexDefinitionStorage.IndexNames)
			{
				OpenIndexOnStartup(documentDatabase, indexName);
			}
		}

		private void OpenIndexOnStartup(DocumentDatabase documentDatabase, string indexName)
		{
			if (indexName == null) throw new ArgumentNullException("indexName");

			startupLog.Debug("Loading saved index {0}", indexName);

			var indexDefinition = indexDefinitionStorage.GetIndexDefinition(indexName);
			if (indexDefinition == null)
				return;

			Index indexImplementation;
			bool resetTried = false;
			while (true)
			{
				try
				{
					var luceneDirectory = OpenOrCreateLuceneDirectory(indexDefinition, createIfMissing: resetTried);
					indexImplementation = CreateIndexImplementation(indexName, indexDefinition, luceneDirectory);
					var suggestionsForIndex = Path.Combine(configuration.IndexStoragePath, "Raven-Suggestions", indexName);
					if (Directory.Exists(suggestionsForIndex))
					{
						foreach (var directory in Directory.GetDirectories(suggestionsForIndex))
						{
							IndexSearcher searcher;
							using(indexImplementation.GetSearcher(out searcher))
							{
								var key = Path.GetFileName(directory);
								var decodedKey = MonoHttpUtility.UrlDecode(key);
								var lastIndexOfDash = decodedKey.LastIndexOf('-');
								var accuracy = float.Parse(decodedKey.Substring(lastIndexOfDash+1));
								var lastIndexOfDistance = decodedKey.LastIndexOf('-', lastIndexOfDash - 1);
								StringDistanceTypes distanceType;
								Enum.TryParse(decodedKey.Substring(lastIndexOfDistance+1, lastIndexOfDash - lastIndexOfDistance-1),
								                             true, out distanceType);
								var field = decodedKey.Substring(0, lastIndexOfDistance );
								var extension = new SuggestionQueryIndexExtension(
									Path.Combine(configuration.IndexStoragePath, "Raven-Suggestions", indexName, key), searcher.GetIndexReader(),
									SuggestionQueryRunner.GetStringDistance(distanceType),
									field,
									accuracy);
								indexImplementation.SetExtension(key, extension);
							}
						}
					}
					break;
				}
				catch (Exception e)
				{
					if (resetTried)
						throw new InvalidOperationException("Could not open / create index" + indexName + ", reset already tried", e);
					resetTried = true;
					startupLog.WarnException("Could not open index " + indexName + ", forcibly resetting index", e);
					try
					{
						documentDatabase.TransactionalStorage.Batch(accessor =>
						{
							accessor.Indexing.DeleteIndex(indexName);
							accessor.Indexing.AddIndex(indexName, indexDefinition.IsMapReduce);
						});

						var indexDirectory = indexName;
						var indexFullPath = Path.Combine(path, MonoHttpUtility.UrlEncode(indexDirectory));
						IOExtensions.DeleteDirectory(indexFullPath);
					}
					catch (Exception exception)
					{
						throw new InvalidOperationException("Could not reset index " + indexName, exception);
					}
				}
			}
			indexes.TryAdd(indexName, indexImplementation);
		}


		protected Lucene.Net.Store.Directory OpenOrCreateLuceneDirectory(
			IndexDefinition indexDefinition, 
			string indexName = null,
			bool createIfMissing = true)
		{
			Lucene.Net.Store.Directory directory;
			if (indexDefinition.IsTemp || configuration.RunInMemory)
			{
				directory = new RAMDirectory();
				new IndexWriter(directory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Close(); // creating index structure
			}
			else
			{
				var indexDirectory = indexName ?? IndexDefinitionStorage.FixupIndexName(indexDefinition.Name, path);
				var indexFullPath = Path.Combine(path, MonoHttpUtility.UrlEncode(indexDirectory));
				directory = FSDirectory.Open(new DirectoryInfo(indexFullPath));

				if (!IndexReader.IndexExists(directory))
				{
					if(createIfMissing == false)
						throw new InvalidOperationException("Index does not exists: " + indexDirectory);

					WriteIndexVersion(directory);

					//creating index structure if we need to
					new IndexWriter(directory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Close();
				}
				else
				{
					EnsureIndexVersionMatches(indexName, directory);
					if (directory.FileExists("write.lock")) // we had an unclean shutdown
					{
						if(configuration.ResetIndexOnUncleanShutdown)
							throw new InvalidOperationException("Rude shutdown detected on: " + indexDirectory);

						CheckIndexAndRecover(directory, indexDirectory);
						IndexWriter.Unlock(directory);
						// for some reason, just calling ulock doesn't remove this file
						directory.DeleteFile("write.lock");
					}
				}
			}

			return directory;

		}

		private static void WriteIndexVersion(Lucene.Net.Store.Directory directory)
		{
			var indexOutput = directory.CreateOutput("index.version");
			try
			{
				indexOutput.WriteString(IndexVersion);
				indexOutput.Flush();
			}
			finally
			{
				indexOutput.Close();
			}
		}

		private static void EnsureIndexVersionMatches(string indexName, Lucene.Net.Store.Directory directory)
		{
			if (directory.FileExists("index.version") == false)
			{
				throw new InvalidOperationException("Could not find index.version " + indexName + ", resetting index");
			}
			var indexInput = directory.OpenInput("index.version");
			try
			{
				var versionFromDisk = indexInput.ReadString();
				if (versionFromDisk != IndexVersion)
					throw new InvalidOperationException("Index " + indexName + " is of version " + versionFromDisk +
					                                    " which is not compatible with " + IndexVersion + ", resetting index");
			}
			finally
			{
				indexInput.Close();
			}
		}

		private static void CheckIndexAndRecover(Lucene.Net.Store.Directory directory, string indexDirectory)
		{
			startupLog.Warn("Unclean shutdown detected on {0}, checking the index for errors. This may take a while.", indexDirectory);

			var memoryStream = new MemoryStream();
			var stringWriter = new StreamWriter(memoryStream);
			var checkIndex = new CheckIndex(directory);

			if (startupLog.IsWarnEnabled)
				checkIndex.SetInfoStream(stringWriter);

			var sp = Stopwatch.StartNew();
			var status = checkIndex.CheckIndex_Renamed_Method();
			sp.Stop();
			if (startupLog.IsWarnEnabled)
			{
				startupLog.Warn("Checking index {0} took: {1}, clean: {2}", indexDirectory, sp.Elapsed, status.clean);
				memoryStream.Position = 0;

				log.Warn(new StreamReader(memoryStream).ReadToEnd());
			}

			if (status.clean)
				return;

			startupLog.Warn("Attempting to fix index: {0}", indexDirectory);
			sp.Restart();
			checkIndex.FixIndex(status);
			startupLog.Warn("Fixed index {0} in {1}", indexDirectory, sp.Elapsed);
		}

		internal Lucene.Net.Store.Directory MakeRAMDirectoryPhysical(RAMDirectory ramDir, string indexName)
		{
			var newDir = FSDirectory.Open(new DirectoryInfo(Path.Combine(path, MonoHttpUtility.UrlEncode(IndexDefinitionStorage.FixupIndexName(indexName, path)))));
			Lucene.Net.Store.Directory.Copy(ramDir, newDir, true);
			return newDir;
		}

		private Index CreateIndexImplementation(string directoryPath, IndexDefinition indexDefinition, Lucene.Net.Store.Directory directory)
		{
			var viewGenerator = indexDefinitionStorage.GetViewGenerator(indexDefinition.Name);
			var indexImplementation = indexDefinition.IsMapReduce
										? (Index)new MapReduceIndex(directory, directoryPath, indexDefinition, viewGenerator, configuration)
										: new SimpleIndex(directory, directoryPath, indexDefinition, viewGenerator, configuration);

			configuration.Container.SatisfyImportsOnce(indexImplementation);

			return indexImplementation;
		}

		public string[] Indexes
		{
			get { return indexes.Keys.ToArray(); }
		}

		public bool HasIndex(string index)
		{
			if (index == null)
				return false;
			return indexes.ContainsKey(index);
		}

		public void Dispose()
		{
			foreach (var index in indexes.Values)
			{
				index.Dispose();
			}
			dummyAnalyzer.Close();
			if (crashMarker != null)
				crashMarker.Dispose();
		}

		public void DeleteIndex(string name)
		{
			Index value;
			if (indexes.TryGetValue(name, out value) == false)
			{
				log.Info("Ignoring delete for non existing index {0}", name);
				return;
			}
			log.Info("Deleting index {0}", name);
			value.Dispose();
			Index ignored;
			var dirOnDisk = Path.Combine(path, MonoHttpUtility.UrlEncode(name));

			if (!indexes.TryRemove(name, out ignored) || !Directory.Exists(dirOnDisk))
				return;

			IOExtensions.DeleteDirectory(dirOnDisk);
		}

		public void CreateIndexImplementation(IndexDefinition indexDefinition)
		{
			var encodedName = IndexDefinitionStorage.FixupIndexName(indexDefinition.Name, path);
			log.Info("Creating index {0} with encoded name {1}", indexDefinition.Name, encodedName);

			IndexDefinitionStorage.ResolveAnalyzers(indexDefinition);
			AssertAnalyzersValid(indexDefinition);

			indexes.AddOrUpdate(indexDefinition.Name, n =>
			{
				var directory = OpenOrCreateLuceneDirectory(indexDefinition, encodedName);
				return CreateIndexImplementation(encodedName, indexDefinition, directory);
			}, (s, index) => index);
		}

		private static void AssertAnalyzersValid(IndexDefinition indexDefinition)
		{
			foreach (var analyzer in from analyzer in indexDefinition.Analyzers
									 let analyzerType = typeof(StandardAnalyzer).Assembly.GetType(analyzer.Value) ?? Type.GetType(analyzer.Value, throwOnError: false)
									 where analyzerType == null
									 select analyzer)
			{
				throw new ArgumentException(string.Format("Could not create analyzer for field: '{0}' because the type '{1}' was not found", analyzer.Key, analyzer.Value));
			}
		}

		public Query GetLuceneQuery(string index, IndexQuery query, OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.Debug("Query on non existing index {0}", index);
				throw new InvalidOperationException("Index '" + index + "' does not exists");
			}
			var fieldsToFetch = new FieldsToFetch(new string[0], AggregationOperation.None, null);
			return new Index.IndexQueryOperation(value, query, _ => false, fieldsToFetch, indexQueryTriggers).GetLuceneQuery();
		}

		public IEnumerable<IndexQueryResult> Query(
			string index,
			IndexQuery query,
			Func<IndexQueryResult, bool> shouldIncludeInResults,
			FieldsToFetch fieldsToFetch,
			OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.Debug("Query on non existing index '{0}'", index);
				throw new InvalidOperationException("Index '" + index + "' does not exists");
			}

			var indexQueryOperation = new Index.IndexQueryOperation(value, query, shouldIncludeInResults, fieldsToFetch, indexQueryTriggers);
			if (query.Query != null && query.Query.Contains(Constants.IntersectSeperator))
				return indexQueryOperation.IntersectionQuery();
			return indexQueryOperation.Query();
		}

		protected internal static IDisposable EnsureInvariantCulture()
		{
			if (Thread.CurrentThread.CurrentCulture == CultureInfo.InvariantCulture)
				return null;

			var oldCurrentCulture = Thread.CurrentThread.CurrentCulture;
			var oldCurrentUiCulture = Thread.CurrentThread.CurrentUICulture;

			Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
			Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
			return new DisposableAction(() =>
			{
				Thread.CurrentThread.CurrentCulture = oldCurrentCulture;
				Thread.CurrentThread.CurrentUICulture = oldCurrentUiCulture;
			});
		}

		public void RemoveFromIndex(string index, string[] keys, WorkContext context)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.Debug("Removing from non existing index '{0}', ignoring", index);
				return;
			}
			value.Remove(keys, context);
		}

		public void Index(string index,
			AbstractViewGenerator viewGenerator,
			IEnumerable<dynamic> docs,
			WorkContext context,
			IStorageActionsAccessor actions,
			DateTime minimumTimestamp)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.Debug("Tried to index on a non existent index {0}, ignoring", index);
				return;
			}
			using (EnsureInvariantCulture())
			using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
			{
				value.IndexDocuments(viewGenerator, docs, context, actions, minimumTimestamp);
			}
		}

		public void Reduce(string index, AbstractViewGenerator viewGenerator, IEnumerable<object> mappedResults,
						   WorkContext context, IStorageActionsAccessor actions, string[] reduceKeys)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.Debug("Tried to index on a non existent index {0}, ignoring", index);
				return;
			}
			var mapReduceIndex = value as MapReduceIndex;
			if (mapReduceIndex == null)
			{
				log.Warn("Tried to reduce on an index that is not a map/reduce index: {0}, ignoring", index);
				return;
			}
			using (EnsureInvariantCulture())
			{
				mapReduceIndex.ReduceDocuments(viewGenerator, mappedResults, context, actions, reduceKeys);
			}
		}

		public IDisposable GetCurrentIndexSearcher(string indexName, out IndexSearcher searcher)
		{
			return GetIndexByName(indexName).GetSearcher(out searcher);
		}

		private Index GetIndexByName(string indexName)
		{
			var result = indexes.Where(index => System.String.Compare(index.Key, indexName, System.StringComparison.OrdinalIgnoreCase) == 0)
				.Select(x => x.Value)
				.FirstOrDefault();
			if (result == null)
				throw new InvalidOperationException(string.Format("Index '{0}' does not exist", indexName));
			return result;
		}

		public void RunIdleOperations()
		{
			foreach (var value in indexes.Values)
			{
				value.Flush();
				value.MergeSegments(); // noop if previously merged
			}
		}

		public void FlushMapIndexes()
		{
			foreach (var value in indexes.Values.Where(value => !value.IsMapReduce))
			{
				value.Flush();
			}
		}

		public void FlushReduceIndexes()
		{
			foreach (var value in indexes.Values.Where(value => value.IsMapReduce))
			{
				value.Flush();
			}
		}

		public IIndexExtension GetIndexExtension(string index, string indexExtensionKey)
		{
			return GetIndexByName(index).GetExtension(indexExtensionKey);
		}

		public void SetIndexExtension(string indexName, string indexExtensionKey, IIndexExtension suggestionQueryIndexExtension)
		{
			GetIndexByName(indexName).SetExtension(indexExtensionKey, suggestionQueryIndexExtension);
		}

		public Index GetIndexInstance(string indexName)
		{
			return indexes.Where(index => System.String.Compare(index.Key, indexName, System.StringComparison.OrdinalIgnoreCase) == 0)
				.Select(x => x.Value)
				.FirstOrDefault();
		}
	}
}
