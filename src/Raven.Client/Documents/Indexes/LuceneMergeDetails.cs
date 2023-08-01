namespace Raven.Client.Documents.Indexes
{
    public sealed class LuceneMergeDetails
    {
        public int TotalMergesCount { get; set; }

        public int ExecutedMergesCount { get; set; }

        public long MergedFilesCount { get; set; }

        public long MergedDocumentsCount { get; set; }
    }
}
