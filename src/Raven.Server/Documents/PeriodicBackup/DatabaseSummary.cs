namespace Raven.Server.Documents.PeriodicBackup
{
    public class DatabaseSummary
    {
        public long DocumentsCount { get; set; }

        public long AttachmentsCount { get; set; }

        public long RevisionsCount { get; set; }

        public long IndexesCount { get; set; }

        public long IdentitiesCount { get; set; }
    }
}