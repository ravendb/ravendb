namespace Raven.Client.Documents.Indexes
{
    public class StorageCommitDetails 
    {
        public int NumberOfModifiedPages { get; set; }

        public int NumberOf4KbsWrittenToDisk { get; set; }
    }
}
