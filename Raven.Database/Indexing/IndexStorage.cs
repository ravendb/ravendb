//-----------------------------------------------------------------------
// <copyright file="IndexStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
		private readonly IndexDefinitionStorage indexDefinitionStorage;
		private readonly InMemoryRavenConfiguration configuration;
		private readonly string path;
		private readonly ConcurrentDictionary<string, Index> indexes = new ConcurrentDictionary<string, Index>(StringComparer.InvariantCultureIgnoreCase);
		private static readonly Logger log = LogManager.GetCurrentClassLogger();
		private readonly Analyzer dummyAnalyzer = new SimpleAnalyzer();

		public IndexStorage(IndexDefinitionStorage indexDefinitionStorage, InMemoryRavenConfiguration configuration)
		{
			this.indexDefinitionStorage = indexDefinitionStorage;
			this.configuration = configuration;
			path = configuration.IndexStoragePath;

			if (Directory.Exists(path) == false && configuration.RunInMemory == false)
				Directory.CreateDirectory(path);

			foreach (var indexDirectory in indexDefinitionStorage.IndexNames)
			{
				log.Debug("Loading saved index {0}", indexDirectory);

				var indexDefinition = indexDefinitionStorage.GetIndexDefinition(indexDirectory);
				if (indexDefinition == null)
					continue;

				var luceneDirectory = OpenOrCreateLuceneDirectory(indexDefinition);
				var indexImplementation = CreateIndexImplementation(indexDirectory, indexDefinition, luceneDirectory);
				indexes.TryAdd(indexDirectory, indexImplementation);
			}
		}


		protected Lucene.Net.Store.Directory OpenOrCreateLuceneDirectory(IndexDefinition indexDefinition, string indexName = null)
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
					//creating index structure if we need to
					new IndexWriter(directory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Close();
				}
				else
				{
					// forcefully unlock locked indexes if any
					if (IndexWriter.IsLocked(directory))
						IndexWriter.Unlock(directory);
				}
			}

			return directory;
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
										? (Index)new MapReduceIndex(directory, directoryPath, indexDefinition, viewGenerator)
										: new SimpleIndex(directory, directoryPath, indexDefinition, viewGenerator);

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

		#region IDisposable Members

		public void Dispose()
		{
			foreach (var index in indexes.Values)
			{
				index.Dispose();
			}
			dummyAnalyzer.Close();
		}

		#endregion

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
			return new Index.IndexQueryOperation(value, query, shouldIncludeInResults, fieldsToFetch, indexQueryTriggers).Query();
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
			var result = indexes.Where(index => string.Compare(index.Key, indexName, true) == 0)
				.Select(x => x.Value)
				.FirstOrDefault();
			if (result == null)
				throw new InvalidOperationException(string.Format("Index '{0}' does not exist", indexName));
			return result;
		}

		public void FlushMapIndexes(bool optimize = false)
		{
			foreach (var value in indexes.Values)
			{
				if (value.IsMapReduce)
					continue;
				value.Flush(optimize);
			}
		}

		public void FlushReduceIndexes(bool optimize = false)
		{
			foreach (var value in indexes.Values)
			{
				if (value.IsMapReduce == false)
					continue;
				value.Flush(optimize);
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
	}
}
