using System;
using System.Diagnostics.CodeAnalysis;
using Sparrow.Json;
using Voron;
using Voron.Impl;

namespace Corax
{
    public class IndexWriter : IDisposable // single threaded, controlled by caller
    {
        private readonly StorageEnvironment _environment;
        private Transaction _transaction;

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index writer with opening semantics and also every new
        // writer becomes essentially a unit of work which makes reusing assets tracking more explicit.
        public IndexWriter([NotNull] StorageEnvironment environment, Transaction transaction)
        {
            _environment = environment;
            _transaction = transaction;
        }

        public void Index(BlittableJsonReaderObject item)
        {

        }

        public void Commit()
        {
            // Commit the current index writer transaction. 

        }

        public void Dispose()
        {
            _transaction?.Dispose();
        }
    }
}
