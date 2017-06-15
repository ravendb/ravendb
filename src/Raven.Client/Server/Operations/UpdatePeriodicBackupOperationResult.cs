namespace Raven.Client.Server.Operations
{
    public class UpdatePeriodicBackupOperationResult
    {
        public long RaftCommandIndex { get; set; }

        public long TaskId { get; set; }
    }
}