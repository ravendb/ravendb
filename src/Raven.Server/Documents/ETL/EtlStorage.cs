using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.ETL
{
    public unsafe class EtlStorage
    {
        protected readonly Logger Logger;
        
        private TransactionContextPool _contextPool;

        public EtlStorage(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger<EtlStorage>(resourceName);
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _contextPool = contextPool;
        }

        public void StoreLastProcessedEtag(EtlDestination destination, string name, long etag)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var tree = tx.InnerTransaction.CreateTree(GetTreeName(destination));

                using (Slice.From(context.Allocator, name, ByteStringType.Immutable, out var transformationSlice))
                using (Slice.External(context.Allocator, (byte*)&etag, sizeof(long), out var etagSlice))
                    tree.Add(transformationSlice, etagSlice);

                tx.Commit();
            }
        }

        public long GetLastProcessedEtag(EtlDestination destination, string name)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var tree = tx.InnerTransaction.ReadTree(GetTreeName(destination));

                var readResult = tree?.Read(name);
                if (readResult == null)
                    return 0;

                return readResult.Reader.ReadLittleEndianInt64();
            }
        }

        public void Remove(EtlDestination destination, string name = null)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var treeName = GetTreeName(destination);
                var tree = tx.InnerTransaction.ReadTree(treeName);

                if (tree == null)
                    return;

                if (name != null)
                    tree.Delete(name);
                else
                    tx.InnerTransaction.DeleteTree(treeName);

                tx.Commit();
            }
        }

        private string GetTreeName(EtlDestination destination)
        {
            return $"__etl/{destination.UniqueName}";
        }
    }
}