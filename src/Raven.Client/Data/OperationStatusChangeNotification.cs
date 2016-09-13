namespace Raven.Client.Data
{
    public class OperationStatusChangeNotification : Notification
    {
        public long OperationId { get; set; }

        public OperationState State { get; set; }
    }
}