using Raven.Server.Documents;
using Raven.Server.Json;

using Voron;
using Voron.Impl;

namespace Raven.Server.ServerWide.Context
{
    public class TransactionOperationContext : MemoryOperationContext
    {
        private readonly StorageEnvironment _environment;

        public Transaction Transaction;

        public TransactionOperationContext(UnmanagedBuffersPool pool, StorageEnvironment environment)
            : base(pool)
        {
            _environment = environment;
        }

        public Transaction OpenReadTransaction()
        {
            return Transaction = _environment.ReadTransaction();
        }

        public Transaction OpenWriteTransaction()
        {
            return Transaction = _environment.WriteTransaction();
        }

        public override void Reset()
        {
            base.Reset();

            Transaction?.Dispose();
            Transaction = null;
        }
    }
}