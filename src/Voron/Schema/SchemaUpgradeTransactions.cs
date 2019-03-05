using System;
using Voron.Impl;

namespace Voron.Schema
{
    public class SchemaUpgradeTransactions : IDisposable
    {
        private readonly StorageEnvironment _env;

        public SchemaUpgradeTransactions(StorageEnvironment env)
        {
            _env = env;

            OpenRead();
            OpenWrite();
        }

        public Transaction Read { get; private set; }
        public Transaction Write { get; private set; }

        public void OpenRead()
        {
            var readPersistentContext = new TransactionPersistentContext(true);

            Read = new Transaction(_env.NewLowLevelTransaction(readPersistentContext, TransactionFlags.Read));
        }

        private void OpenWrite()
        {
            var writePersistentContext = new TransactionPersistentContext(true);

            Write = new Transaction(_env.NewLowLevelTransaction(writePersistentContext, TransactionFlags.ReadWrite));
        }

        public void Commit()
        {
            using (Write)
            {
                Write?.Commit();
            }

            Write = null;
        }

        public void Renew()
        {
            Read?.Dispose();
            Write?.Dispose();

            OpenRead();
            OpenWrite();
        }

        public void Dispose()
        {
            Commit();

            using (Read)
            {
                Read?.Commit();
            }

            Read = null;
        }
    }
}
