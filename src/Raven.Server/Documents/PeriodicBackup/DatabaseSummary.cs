namespace Raven.Server.Documents.PeriodicBackup
{
    public class DatabaseSummary
    {
        public long DocumentsCount { get; set; }

        public long AttachmentsCount { get; set; }

        public long RevisionsCount { get; set; }

        public long ConflictsCount { get; set; }

        public long CounterEntriesCount { get; set; }

    }
}
