using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents
{
    public class IndexesAndTransformersStorage
    {
        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private readonly TableSchema _indexesSchema = new TableSchema();

        public IndexesAndTransformersStorage(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger<IndexesAndTransformersStorage>(resourceName);
            _indexesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _environment = environment;
            _contextPool = contextPool;

            TransactionOperationContext context;
            using (contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                _indexesSchema.Create(tx, IndexesAndTransformersStorage.IndexesSchema.IndexesTree);

                tx.Commit();
            }
        }

        public void OnIndexCreated()
        {
            
        }

        public void OnIndexDeleted()
        {
            
        }

        public static class IndexesSchema
        {
            public const string IndexesTree = "Indexes";
        }
    }
}