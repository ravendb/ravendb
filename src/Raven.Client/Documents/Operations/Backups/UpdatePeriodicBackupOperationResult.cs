namespace Raven.Client.Documents.Operations.Backups
{
    public sealed class UpdatePeriodicBackupOperationResult
    {
        public long RaftCommandIndex { get; set; }

        public long TaskId { get; set; }
    }
}
