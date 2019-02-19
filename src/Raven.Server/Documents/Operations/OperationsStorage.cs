using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Operations
{
    public class OperationsStorage
    {
        private StorageEnvironment _environment;
        private TransactionContextPool _contextPool;

        private static readonly Slice NextOperationId;
        private static readonly Slice OperationsTree;

        static OperationsStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Operations", ByteStringType.Immutable, out OperationsTree);
                Slice.From(ctx, "NextOperationId", ByteStringType.Immutable, out NextOperationId);
            }
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _environment = environment;
            _contextPool = contextPool;

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                tx.CreateTree(OperationsTree);
                tx.Commit();
            }
        }

        public long GetNextOperationId()
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                var operationsTree = tx.ReadTree(OperationsTree);
                var id = operationsTree.Increment(NextOperationId, 1);

                tx.Commit();

                return id;
            }
        }
    }
}
