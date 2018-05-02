using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Util;

namespace Raven.Client.Documents.Operations
{
    public class CountersOperationExecutor
    {
        private readonly DocumentStoreBase _store;
        private readonly string _databaseName;
        private readonly OperationExecutor _operations;

        public CountersOperationExecutor(DocumentStoreBase store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName ?? store.Database;
            _operations = _store.Operations.ForDatabase(_databaseName);
        }

        public CountersOperationExecutor ForDatabase(string databaseName)
        {
            if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new CountersOperationExecutor(_store, databaseName);
        }

        public void Batch(CounterBatch counterBatch)
        {
            AsyncHelpers.RunSync(() => BatchAsync(counterBatch));
        }

        public async Task BatchAsync(CounterBatch counterBatch)
        {
            await _operations.SendAsync(new CounterBatchOperation(counterBatch)).ConfigureAwait(false);
        }

        public void Increment(string docId, string name, long delta = 1)
        {
            AsyncHelpers.RunSync(() => IncrementAsync(docId, name, delta));

        }

        public async Task IncrementAsync(string docId, string name, long delta = 1)
        {
            CounterBatch counterBatch = new CounterBatch()
            {
                Counters = new List<CounterOperation>
                {
                    new CounterOperation
                    {
                        DocumentId = docId,
                        CounterName = name,
                        Delta = delta
                    }
                }
            };

            await _operations.SendAsync(new CounterBatchOperation(counterBatch)).ConfigureAwait(false);
        }
    }
}
