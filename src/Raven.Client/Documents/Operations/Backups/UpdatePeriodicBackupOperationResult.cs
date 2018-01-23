namespace Raven.Client.Documents.Operations.Backups
{
    public class UpdatePeriodicBackupOperationResult
    {
        public long RaftCommandIndex { get; set; }

        public long TaskId { get; set; }
    }
}
