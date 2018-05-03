using System;
using System.Collections.Generic;
using System.Linq;
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

        public CountersDetail Batch(CounterBatch counterBatch)
        {
            return AsyncHelpers.RunSync(() => BatchAsync(counterBatch));
        }

        public async Task<CountersDetail> BatchAsync(CounterBatch counterBatch)
        {
            return await _operations.SendAsync(new CounterBatchOperation(counterBatch)).ConfigureAwait(false);
        }

        public CountersDetail Increment(string docId, string name, long delta = 1)
        {
            return AsyncHelpers.RunSync(() => IncrementAsync(docId, name, delta));
        }

        public async Task<CountersDetail> IncrementAsync(string docId, string name, long delta = 1)
        {
            var counterBatch = new CounterBatch
            {
                Documents = new List<DocumentCountersOperation>()
                {
                    new DocumentCountersOperation
                    {
                        DocumentId = docId,
                        Operations = new List<CounterOperation>
                        {
                            new CounterOperation
                            {
                                Type = CounterOperationType.Increment,
                                CounterName = name,
                                Delta = delta
                            }
                        }
                    }
                }
            };

            return await _operations.SendAsync(new CounterBatchOperation(counterBatch)).ConfigureAwait(false);
        }

        public long? Get(string docId, string counterName)
        {
            return AsyncHelpers.RunSync(() => GetAsync(docId, counterName));
        }

        public async Task<long?> GetAsync(string docId, string counterName)
        {
            var details = await _operations.SendAsync(new GetCountersOperation(docId, new[] { counterName })).ConfigureAwait(false);
            if (details.Counters.Count == 0)
                return null;
            return details.Counters[0].TotalValue;
        }

        public Dictionary<string, long> Get(string docId, IEnumerable<string> counters)
        {
            return AsyncHelpers.RunSync(() => GetAsync(docId, counters));
        }

        public async Task<Dictionary<string, long>> GetAsync(string docId, IEnumerable<string> counters)
        {
            var result = new Dictionary<string, long>();
            var details = await _operations.SendAsync(new GetCountersOperation(docId, counters.ToArray())).ConfigureAwait(false);

            foreach (var counterDetail in details.Counters)
            {
                result[counterDetail.CounterName] = counterDetail.TotalValue;
            }

            return result;
        }

        public void Delete(string docId, string counter)
        {
            AsyncHelpers.RunSync(() => DeleteAsync(docId, counter));
        }

        public async Task DeleteAsync(string docId, string counter)
        {
            var counterBatch = new CounterBatch
            {
                Documents = new List<DocumentCountersOperation>()
                {
                    new DocumentCountersOperation
                    {
                        DocumentId = docId,
                        Operations = new List<CounterOperation>
                        {
                            new CounterOperation
                            {
                                Type = CounterOperationType.Delete,
                                CounterName = counter
                            }
                        }
                    }
                }
            };

            await _operations.SendAsync(new CounterBatchOperation(counterBatch)).ConfigureAwait(false);
        }
    }
}
