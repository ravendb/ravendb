using System;
using System.Threading.Tasks;
using Raven.Abstractions.Counters.Notifications;
using Raven.Client.Changes;

namespace Raven.Client.Counters.Changes
{
    public class CountersConnectionState : ConnectionStateBase
    {
        private readonly Func<CountersConnectionState, Task> ensureConnection;

        public CountersConnectionState(Func<Task> disconnectAction, Func<CountersConnectionState, Task> ensureConnection, Task task)
            : base(disconnectAction, task)
        {
            this.ensureConnection = ensureConnection;
        }

        protected override Task EnsureConnection()
        {
            return ensureConnection(this);
        }

        public event Action<CounterChange> OnChangeNotification = (x) => { };
        public void Send(CounterChange changeChange)
        {
            var onCounterChangeNotification = OnChangeNotification;
            onCounterChangeNotification?.Invoke(changeChange);
        }

        public event Action<StartingWithChange> OnCountersStartingWithNotification = (x) => { };
        public void Send(StartingWithChange changeChange)
        {
            var onCounterChangeNotification = OnCountersStartingWithNotification;
            onCounterChangeNotification?.Invoke(changeChange);
        }

        public event Action<InGroupChange> OnCountersInGroupNotification = (x) => { };
        public void Send(InGroupChange changeChange)
        {
            var onCounterChangeNotification = OnCountersInGroupNotification;
            onCounterChangeNotification?.Invoke(changeChange);
        }

        public event Action<BulkOperationChange> OnBulkOperationNotification = (x) => { };
        public void Send(BulkOperationChange bulkOperationChange)
        {
            var onBulkOperationNotification = OnBulkOperationNotification;
            onBulkOperationNotification?.Invoke(bulkOperationChange);
        }
    }
}