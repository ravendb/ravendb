using System;
using System.Collections;
using Voron;
using Voron.Impl;

namespace Corax
{
    public class IndexSearcher : IDisposable
    {
        private readonly StorageEnvironment _environment;
        private readonly Transaction _transaction;

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index searcher with opening semantics and also every new
        // searcher becomes essentially a unit of work which makes reusing assets tracking more explicit.
        public IndexSearcher(StorageEnvironment environment, Transaction transaction)
        {
            _environment = environment;
            _transaction = transaction;
        }

        public IEnumerable Query()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            _transaction?.Dispose();
        }
    }
}
