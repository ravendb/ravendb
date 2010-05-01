using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using System.Web;
using log4net;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Store;
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
		private readonly string path;
		private readonly ConcurrentDictionary<string, Index> indexes = new ConcurrentDictionary<string, Index>();
		private readonly ILog log = LogManager.GetLogger(typeof (IndexStorage));

		public IndexStorage(IndexDefinitionStorage indexDefinitionStorage, TransactionalStorage transactionalStorage, string dataDir)
		{
			path = Path.Combine(dataDir,"Indexes");
			if (Directory.Exists(path) == false)
				Directory.CreateDirectory(path);

			transactionalStorage.Batch(actions =>
			{
				string[] indexNames = actions.GetIndexesStats().Select(x => x.Name).ToArray();

				foreach (var indexDirectory in indexNames)
				{
					log.DebugFormat("Loading saved index {0}", indexDirectory);
				
					var indexDefinition = indexDefinitionStorage.GetIndexDefinition(indexDirectory);
					if (indexDefinition == null)
						continue;
					indexes.TryAdd(indexDirectory,
					               CreateIndexImplementation(indexDirectory, indexDefinition,
															 FSDirectory.Open(new DirectoryInfo(Path.Combine(path, HttpUtility.UrlEncode(indexDirectory))))));
				}
			});

		}

		private static Index CreateIndexImplementation(string name, IndexDefinition indexDefinition, Lucene.Net.Store.Directory directory)
		{
			return indexDefinition.IsMapReduce
				? (Index) new MapReduceIndex(directory, name, indexDefinition)
				: new SimpleIndex(directory, name, indexDefinition);
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
			var dirOnDisk = Path.Combine(path, HttpUtility.UrlEncode(name));
			
			if (!indexes.TryRemove(name, out ignored) || !Directory.Exists(dirOnDisk)) 
				return;

			for (int i = 0; i < 15; i++)
			{
				try
				{
					Directory.Delete(dirOnDisk, true);
					break;
				}
				catch (IOException)
				{
					Thread.Sleep(100);
				}
			}
		}

		public void CreateIndexImplementation(string name, IndexDefinition indexDefinition)
		{
			log.InfoFormat("Creating index {0}", name);

			indexes.AddOrUpdate(name, n =>
			{
				var directory = FSDirectory.Open(new DirectoryInfo(Path.Combine(path, HttpUtility.UrlEncode(name))));
				//creating index structure
				new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), IndexWriter.MaxFieldLength.UNLIMITED).
					Close();
				return CreateIndexImplementation(name, indexDefinition, directory);
			}, (s, index) => index);
		}

		public IEnumerable<IndexQueryResult> Query(string index, IndexQuery query)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.DebugFormat("Query on non existing index {0}", index);
				throw new InvalidOperationException("Index " + index + " does not exists");
			}
			return value.Query(query);
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

		public void Index(string index, AbstractViewGenerator viewGenerator, IEnumerable<dynamic> docs, WorkContext context,
		                  DocumentStorageActions actions)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.DebugFormat("Tried to index on a non existant index {0}, ignoring", index);
				return;
			}
			value.IndexDocuments(viewGenerator, docs, context, actions);
		}

		public void Reduce(string index, AbstractViewGenerator viewGenerator, IEnumerable<object> mappedResults,
		                   WorkContext context, DocumentStorageActions actions, string reduceKey)
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
			mapReduceIndex.ReduceDocuments(viewGenerator, mappedResults, context, actions, reduceKey);
		}
	}
}