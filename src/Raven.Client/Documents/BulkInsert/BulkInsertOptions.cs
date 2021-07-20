namespace Raven.Client.Documents.BulkInsert
{
    public class BulkInsertOptions
    {
        /// <summary>
        /// Determines whether we should skip overwriting a document when it is updated by exactly the same document (by comparing the content and the metadata)
        /// </summary>
        public bool SkipOverwriteIfUnchanged;
    }
}
