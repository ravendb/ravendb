using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using log4net;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Directory = System.IO.Directory;

namespace Raven.Database.Indexing
{
    /// <summary>
    ///   Thread safe, single instance for the entire application
    /// </summary>
    public class IndexStorage : CriticalFinalizerObject, IDisposable
    {
        private readonly ConcurrentDictionary<string, Index> indexes = new ConcurrentDictionary<string, Index>();
        private readonly ILog log = LogManager.GetLogger(typeof (IndexStorage));
        private readonly string path;

        public IndexStorage(string path)
        {
            this.path = Path.Combine(path, "Index");
            if (Directory.Exists(this.path) == false)
                Directory.CreateDirectory(this.path);
            log.DebugFormat("Initializing index storage at {0}", this.path);
            foreach (var index in Directory.GetDirectories(this.path))
            {
                log.DebugFormat("Loading saved index {0}", index);
                var name = Path.GetFileName(index);
                indexes.TryAdd(name, new Index(FSDirectory.GetDirectory(index, false), name));
            }
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
            var indexDir = Path.Combine(path, name);
            if (indexes.TryRemove(name, out ignored) && Directory.Exists(indexDir))
            {
                Directory.Delete(indexDir, true);
            }
        }

        public void CreateIndex(string name)
        {
            log.InfoFormat("Creating index {0}", name);
            indexes.AddOrUpdate(name, BuildIndex, (s, index) => index);
        }

        private Index BuildIndex(string name)
        {
            var directory = FSDirectory.GetDirectory(Path.Combine(path, name), true);
            new IndexWriter(directory, new StandardAnalyzer()).Close(); //creating index structure
            return new Index(directory, name);
        }

        public IEnumerable<IndexQueryResult> Query(string index, string query, int start, int pageSize, Reference<int> totalSize, string[] fieldsToFetch)
        {
            Index value;
            if (indexes.TryGetValue(index, out value) == false)
            {
                log.DebugFormat("Query on non existing index {0}", index);
                throw new InvalidOperationException("Index " + index + " does not exists");
            }
            return value.Query(query, start, pageSize, totalSize, fieldsToFetch);
        }

        public void RemoveFromIndex(string index, string[] keys)
        {
            Index value;
            if (indexes.TryGetValue(index, out value) == false)
            {
                log.DebugFormat("Removing from non existing index {0}, ignoring", index);
                return;
            }
            value.Remove(keys);
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
    }
}