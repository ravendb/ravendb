using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using log4net;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Directory = System.IO.Directory;
using Version = Lucene.Net.Util.Version;

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
		        indexes.TryAdd(indexDirectory,
		                       CreateIndexImplementation(indexDirectory, indexDefinition,
		                                                 OpenOrCreateLuceneDirectory(indexDirectory)));
		    }
		}

		
		protected Lucene.Net.Store.Directory OpenOrCreateLuceneDirectory(string indexDirectory)
		{
			Lucene.Net.Store.Directory directory;
			if (configuration.RunInMemory)
				directory = new RAMDirectory();
			else
				directory = FSDirectory.Open(new DirectoryInfo(Path.Combine(path, MonoHttpUtility.UrlEncode(indexDirectory))));
            //creating index structure if we need to
	        var standardAnalyzer = new StandardAnalyzer(Version.LUCENE_29);
	        try
	        {
	            new IndexWriter(directory, standardAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).
	                Close();
	        }
	        finally
	        {
	            standardAnalyzer.Close();
	        }
            return directory;
		}

		private Index CreateIndexImplementation(string name, IndexDefinition indexDefinition, Lucene.Net.Store.Directory directory)
		{
		    var viewGenerator = indexDefinitionStorage.GetViewGenerator(name);
		    return indexDefinition.IsMapReduce
                ? (Index)new MapReduceIndex(directory, name, indexDefinition, viewGenerator)
                : new SimpleIndex(directory, name, indexDefinition, viewGenerator);
		}

		public string[] Indexes
		{
			get { return indexes.Keys.ToArray(); }
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

		public void CreateIndexImplementation(string name, IndexDefinition indexDefinition)
		{
			log.InfoFormat("Creating index {0}", name);

			indexes.AddOrUpdate(name, n =>
			{
				var directory = OpenOrCreateLuceneDirectory(name);
				return CreateIndexImplementation(name, indexDefinition, directory);
			}, (s, index) => index);
		}

		public IEnumerable<IndexQueryResult> Query(
            string index, 
            IndexQuery query, 
            Func<IndexQueryResult, bool> shouldIncludeInResults)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.DebugFormat("Query on non existing index {0}", index);
				throw new InvalidOperationException("Index " + index + " does not exists");
			}
			return value.Query(query, shouldIncludeInResults);
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
            var result = indexes.Where(index => string.Compare(index.Key, indexName, true) == 0)
                .Select(x => x.Value)
                .FirstOrDefault();
            if (result == null)
                throw new InvalidOperationException(string.Format("Index '{0}' does not exist", indexName));

            return result.Searcher;
        }
	}
}
