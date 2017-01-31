using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron;

namespace Raven.Server.Documents.Operations
{
    public class OperationsStorage
    {
        private StorageEnvironment _environment;
        private TransactionContextPool _contextPool;

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _environment = environment;
            _contextPool = contextPool;

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                tx.CreateTree(OperationsSchema.OperationsTree);
                tx.Commit();
            }
        }

        public long GetNextOperationId()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                var operationsTree = tx.ReadTree(OperationsSchema.OperationsTree);
                var id = operationsTree.Increment(OperationsSchema.NextOperationId, 1);

                tx.Commit();

                return id;
            }
        }

        public static class OperationsSchema
        {
            public const string OperationsTree = "Operations";

            public static readonly Slice NextOperationId;

            static OperationsSchema()
            {
                Slice.From(StorageEnvironment.LabelsContext, "NextOperationId", ByteStringType.Immutable, out NextOperationId);
            }
        }
    }
}