namespace Raven.Client.Server.Operations
{
    public class UpdatePeriodicBackupOperationResult
    {
        public long ETag { get; set; }

        public long TaskId { get; set; }
    }
}