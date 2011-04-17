//-----------------------------------------------------------------------
// <copyright file="IndexStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using log4net;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Queries;
using Raven.Database.Storage;
using Directory = System.IO.Directory;
using Version = Lucene.Net.Util.Version;
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
		private readonly ILog log = LogManager.GetLogger(typeof (IndexStorage));
		private static readonly Analyzer dummyAnalyzer = new SimpleAnalyzer();

        public IndexStorage(IndexDefinitionStorage indexDefinitionStorage, InMemoryRavenConfiguration configuration)
		{
            this.indexDefinitionStorage = indexDefinitionStorage;
            this.configuration = configuration;
		    path = Path.Combine(configuration.DataDirectory, "Indexes");

            if (Directory.Exists(path) == false && configuration.RunInMemory == false)
		        Directory.CreateDirectory(path);

		    foreach (var indexDirectory in indexDefinitionStorage.IndexNames)
		    {
		        log.DebugFormat("Loading saved index {0}", indexDirectory);

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
				directory = new RAMDirectory();
			else
			{
				var indexDirectory = indexName ?? IndexDefinitionStorage.FixupIndexName(indexDefinition.Name, path);
				directory = FSDirectory.Open(new DirectoryInfo(Path.Combine(path, MonoHttpUtility.UrlEncode(indexDirectory))));
			}

			//creating index structure if we need to
	        try
	        {
				new IndexWriter(directory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).
	                Close();
	        }
	        catch
	        {
	        }
			return directory;
		}

		internal Lucene.Net.Store.Directory MakeRAMDirectoryPhysical(RAMDirectory ramDir, string indexName)
		{
			var newDir =  FSDirectory.Open(new DirectoryInfo(Path.Combine(path, MonoHttpUtility.UrlEncode(IndexDefinitionStorage.FixupIndexName(indexName, path)))));
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
            return indexes.ContainsKey(index);
        }

		#region IDisposable Members

		public void Dispose()
		{
			foreach (var index in indexes.Values)
			{
				index.Dispose();
			}
		}

		#endregion

		public void DeleteIndex(string name)
		{
			Index value;
			if (indexes.TryGetValue(name, out value) == false)
			{
				log.InfoFormat("Ignoring delete for non existing index {0}", name);
				return;
			}
			log.InfoFormat("Deleting index {0}", name);
			value.Dispose();
			Index ignored;
			var dirOnDisk = Path.Combine(path, MonoHttpUtility.UrlEncode(name));
			
			if (!indexes.TryRemove(name, out ignored) || !Directory.Exists(dirOnDisk)) 
				return;

			IOExtensions.DeleteDirectory(dirOnDisk);
		}

		public void CreateIndexImplementation(IndexDefinition indexDefinition)
		{
			var encodedName = IndexDefinitionStorage.FixupIndexName(indexDefinition.Name,path);
			log.InfoFormat("Creating index {0} with encoded name {1}", indexDefinition.Name, encodedName);

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
	                                 let analyzerType = typeof (StandardAnalyzer).Assembly.GetType(analyzer.Value) ?? Type.GetType(analyzer.Value, throwOnError: false)
	                                 where analyzerType == null
	                                 select analyzer)
	        {
	            throw new ArgumentException("Could not create analyzer for field: " + analyzer.Key +
	                "' because the type was not found: " + analyzer.Value);
	        }
	    }

	    public IEnumerable<IndexQueryResult> Query(
            string index, 
            IndexQuery query, 
            Func<IndexQueryResult, bool> shouldIncludeInResults,
			FieldsToFetch fieldsToFetch)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.DebugFormat("Query on non existing index {0}", index);
				throw new InvalidOperationException("Index " + index + " does not exists");
			}
	    	return new Index.IndexQueryOperation(value, query, shouldIncludeInResults, fieldsToFetch).Query();
		}

		public void RemoveFromIndex(string index, string[] keys, WorkContext context)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.DebugFormat("Removing from non existing index {0}, ignoring", index);
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
				log.DebugFormat("Tried to index on a non existant index {0}, ignoring", index);
				return;
			}
			value.IndexDocuments(viewGenerator, docs, context, actions, minimumTimestamp);
		}

		public void Reduce(string index, AbstractViewGenerator viewGenerator, IEnumerable<object> mappedResults,
						   WorkContext context, IStorageActionsAccessor actions, string[] reduceKeys)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.DebugFormat("Tried to index on a non existant index {0}, ignoring", index);
				return;
			}
			var mapReduceIndex = value as MapReduceIndex;
			if (mapReduceIndex == null)
			{
				log.WarnFormat("Tried to reduce on an index that is not a map/reduce index: {0}, ignoring", index);
				return;
			}
			mapReduceIndex.ReduceDocuments(viewGenerator, mappedResults, context, actions, reduceKeys);
		}

        internal Index.CurrentIndexSearcher GetCurrentIndexSearcher(string indexName)
        {
        	return GetIndexByName(indexName).Searcher;
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

		public void FlushAllIndexes()
		{
			foreach (var value in indexes.Values)
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
	}
}
