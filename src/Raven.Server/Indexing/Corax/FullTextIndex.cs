using System;
using Raven.Server.Indexing.Corax.Analyzers;
using Raven.Server.Json;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Indexing.Corax
{
    public class FullTextIndex : IDisposable
    {
        private readonly StorageEnvironment _env;

        private readonly TableSchema _entriesSchema = new TableSchema()
            .DefineKey(new TableSchema.SchemaIndexDef
            {
                // TODO: this is standard b+tree, we probably want to use a fixed size tree here
                StartIndex = 0,
                Count = 1
            });

        public TableSchema EntriesSchema => _entriesSchema;
        public StorageEnvironment Env => _env;

        private readonly UnmanagedBuffersPool _pool;
        private IAnalyzer _analyzer;

        public UnmanagedBuffersPool Pool => _pool;

        public IAnalyzer Analyzer => _analyzer;

        public FullTextIndex(StorageEnvironmentOptions options, IAnalyzer defaultAnalyzer)
        {
            try
            {
                _pool = new UnmanagedBuffersPool("Index for " + options.BasePath);
                _env = new StorageEnvironment(options);
                _analyzer = defaultAnalyzer;
                using (var tx = _env.WriteTransaction())
                {
                    tx.CreateTree("Fields");
                    tx.CreateTree("Options");
                    _entriesSchema.Create(tx, "IndexEntries");
                    
                    tx.Commit();
                }
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public Indexer CreateIndexer()
        {
            return new Indexer(this);
        }


        public Searcher CreateSearcher()
        {
            return new Searcher(this);
        }



        public void Dispose()
        {
            _pool?.Dispose();
            _env?.Dispose();
        }
    }
}