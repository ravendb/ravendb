namespace Raven.Server.Documents.PeriodicBackup
{
    public class BackupDatabaseNowResult
    {
        public string ResponsibleNode { get; set; }

        public int OperationId { get; set; }
    }
}