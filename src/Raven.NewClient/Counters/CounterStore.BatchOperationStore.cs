using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.NewClient.Client.Counters.Operations;

namespace Raven.NewClient.Client.Counters
{
    public partial class CounterStore
    {
        public class BatchOperationsStore 
        {
            private readonly CounterStore parent;
            private readonly Lazy<CountersBatchOperation> defaultBatchOperation;
            private readonly ConcurrentDictionary<string, CountersBatchOperation> batchOperations;

            internal BatchOperationsStore(CounterStore parent)
            {
                batchOperations = new ConcurrentDictionary<string, CountersBatchOperation>();
                this.parent = parent;
                if (string.IsNullOrWhiteSpace(parent.Name) == false)
                    defaultBatchOperation = new Lazy<CountersBatchOperation>(() => new CountersBatchOperation(parent, parent.Name));

                OperationId = Guid.NewGuid();
            }

            public CountersBatchOperation this[string storageName]
            {
                get { return GetOrCreateBatchOperation(storageName); }
            }

            private CountersBatchOperation GetOrCreateBatchOperation(string storageName)
            {
                return batchOperations.GetOrAdd(storageName, arg => new CountersBatchOperation(parent, storageName));
            }

            public void Dispose()
            {
                batchOperations.Values
                    .ForEach(operation => operation.Dispose());
                if (defaultBatchOperation != null && defaultBatchOperation.IsValueCreated)
                    defaultBatchOperation.Value.Dispose();
            }

            public void ScheduleChange(string groupName, string counterName, long delta)
            {
                if (string.IsNullOrWhiteSpace(parent.Name))
                    throw new InvalidOperationException("Default counter storage name cannot be empty!");

                defaultBatchOperation.Value.ScheduleChange(groupName, counterName, delta);
            }

            public void ScheduleIncrement(string groupName, string counterName)
            {
                if (string.IsNullOrWhiteSpace(parent.Name))
                    throw new InvalidOperationException("Default counter storage name cannot be empty!");

                defaultBatchOperation.Value.ScheduleIncrement(groupName, counterName);
            }

            public void ScheduleDecrement(string groupName, string counterName)
            {
                if (string.IsNullOrWhiteSpace(parent.Name))
                    throw new InvalidOperationException("Default counter storage name cannot be empty!");

                defaultBatchOperation.Value.ScheduleDecrement(groupName, counterName);
            }

            public async Task FlushAsync()
            {
                if (string.IsNullOrWhiteSpace(parent.Name))
                    throw new InvalidOperationException("Default counter storage name cannot be empty!");

                parent.AssertInitialized();

                await parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);
                await defaultBatchOperation.Value.FlushAsync().ConfigureAwait(false);
            }

            public Guid OperationId { get; private set; }

            public CountersBatchOptions DefaultOptions
            {
                get
                {
                    if (string.IsNullOrWhiteSpace(parent.Name))
                        throw new InvalidOperationException("Default counter storage name cannot be empty!");
                    return defaultBatchOperation.Value.DefaultOptions;
                }
            }
        }
    }
}
