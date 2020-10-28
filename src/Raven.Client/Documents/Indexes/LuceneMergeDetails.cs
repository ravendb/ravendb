namespace Raven.Client.Documents.Indexes
{
    public class LuceneMergeDetails
    {
        public int PendingMergesCount { get; set; }

        public long MergedFilesCount { get; set; }

        public long MergedDocumentsCount { get; set; }
    }
}
