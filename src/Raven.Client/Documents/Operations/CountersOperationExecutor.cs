using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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

        public Task<CountersDetail> BatchAsync(CounterBatch counterBatch, CancellationToken token = default)
        {
            return _operations.SendAsync(new CounterBatchOperation(counterBatch), token: token);
        }

        public CountersDetail Increment(string docId, string name, long delta = 1)
        {
            return AsyncHelpers.RunSync(() => IncrementAsync(docId, name, delta));
        }

        public Task<CountersDetail> IncrementAsync(string docId, string name, long delta = 1, CancellationToken token = default)
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

            return _operations.SendAsync(new CounterBatchOperation(counterBatch), token: token);
        }

        public long? Get(string docId, string counterName)
        {
            return AsyncHelpers.RunSync(() => GetAsync(docId, counterName));
        }

        public async Task<long?> GetAsync(string docId, string counterName, CancellationToken token = default)
        {
            var details = await _operations.SendAsync(new GetCountersOperation(docId, new[] { counterName }), token: token).ConfigureAwait(false);
            if (details.Counters.Count == 0)
                return null;
            return details.Counters[0].TotalValue;
        }

        public Dictionary<string, long?> Get(string docId, IEnumerable<string> counters, CancellationToken token = default)
        {
            return AsyncHelpers.RunSync(() => GetAsync(docId, counters, token));
        }

        public async Task<Dictionary<string, long?>> GetAsync(string docId, IEnumerable<string> counters, CancellationToken token = default)
        {
            var result = new Dictionary<string, long?>();
            var details = await _operations.SendAsync(new GetCountersOperation(docId, counters.ToArray()), token: token).ConfigureAwait(false);

            foreach (var counterDetail in details.Counters)
            {
                result[counterDetail.CounterName] = counterDetail.TotalValue;
            }

            return result;
        }

        public Dictionary<string, long> GetAll(string docId)
        {
            return AsyncHelpers.RunSync(() => GetAllAsync(docId));
        }

        public async Task<Dictionary<string, long>> GetAllAsync(string docId)
        {
            var result = new Dictionary<string, long>();
            var details = await _operations.SendAsync(new GetCountersOperation(docId, new string[0])).ConfigureAwait(false);

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

        public Task DeleteAsync(string docId, string counter, CancellationToken token = default)
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

             return _operations.SendAsync(new CounterBatchOperation(counterBatch), token: token);
        }
    }
}
