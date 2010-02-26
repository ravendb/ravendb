using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using log4net;
using System.Linq;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Directory = System.IO.Directory;

namespace Rhino.DivanDB.Indexing
{
    public class IndexStorage : CriticalFinalizerObject, IDisposable
    {
        private readonly string path;
        private IDictionary<string, Index> indexes = new Dictionary<string, Index>();
        private readonly ILog log = LogManager.GetLogger(typeof(IndexStorage));

        public IndexStorage(string path)
        {
            this.path = Path.Combine(path, "Index");
            if (Directory.Exists(this.path) == false)
                Directory.CreateDirectory(this.path);
            log.DebugFormat("Initializing index storage at {0}", this.path);
            foreach (var index in Directory.GetDirectories(this.path))
            {
                log.DebugFormat("Starting index {0}", index);
                var value = new Index(FSDirectory.GetDirectory(index, false));
                indexes.Add(Path.GetFileName(index), value);
            }
        }

        public string[] Indexes
        {
            get { return indexes.Keys.ToArray(); }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void DeleteIndex(string name)
        {
            Index value;
            if (indexes.TryGetValue(name, out value) == false)
            {
                log.InfoFormat("Ignoring delete for non existing index {0}", name);
                return;
            }
            log.InfoFormat("Deleting delete for index {0}", name);
            value.Dispose();
            indexes = indexes.Where(x => x.Key != name)
                .ToDictionary(x => x.Key, y => y.Value);
            Directory.Delete(Path.Combine(path, name), true);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void CreateIndex(string name)
        {
            log.InfoFormat("Creating index {0}", name);
            var directory = FSDirectory.GetDirectory(Path.Combine(path, name), true);
            new IndexWriter(directory, new StandardAnalyzer()).Close();//creating index structure
            indexes = new Dictionary<string, Index>(indexes)
                      {
                          {name, new Index(directory)}
                      };
        }

        public void Dispose()
        {
            foreach (var index in indexes.Values)
            {
                index.Dispose();
            }
        }

        public IEnumerable<string> Query(string index, string query)
        {
            Index value;
            if (indexes.TryGetValue(index, out value) == false)
            {
                log.DebugFormat("Query on non existing index {0}", index);
                throw new InvalidOperationException("Index " + index + " does not exists");
            }
            return value.Query(query);
        }
    }
}