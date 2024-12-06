namespace Raven.Client.Documents.Indexes
{
    public sealed class StorageCommitDetails 
    {
        public long NumberOfModifiedPages { get; set; }

        public long NumberOf4KbsWrittenToDisk { get; set; }
    }
}
