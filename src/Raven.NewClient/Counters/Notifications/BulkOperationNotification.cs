using System;

namespace Raven.NewClient.Abstractions.Counters.Notifications
{
    public class BulkOperationNotification : CounterStorageNotification
    {
        public Guid OperationId { get; set; }

        public BatchType Type { get; set; }

        public string Message { get; set; }
    }

    public enum BatchType
    {
        Started,
        Ended,
        Error
    }
}
