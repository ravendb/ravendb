namespace Raven.Abstractions.Data
{
    /// <summary>
    /// Options used during BulkInsert execution.
    /// </summary>
    public class BulkInsertOptions
    {
        public BulkInsertOptions()
        {
            BatchSize = 512;
            WriteTimeoutMilliseconds = 15 * 1000;
            ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions
            {
                MaxDocumentsPerChunk = BatchSize*4,
                MaxChunkVolumeInBytes = 8 * 1024 * 1024
                
            };
        }

        /// <summary>
        /// Indicates in existing documents should be overwritten. If not, exception will be thrown.
        /// </summary>
        public bool OverwriteExisting { get; set; }

        /// <summary>
        /// Indicates if referenced documents should be checked in indexes.
        /// </summary>
        public bool CheckReferencesInIndexes { get; set; }

        /// <summary>
        /// Determines whether should skip to overwrite a document when it is updated by exactly the same document (by comparing a content and metadata as well).
        /// </summary>
        public bool SkipOverwriteIfUnchanged { get; set; }

        /// <summary>
        /// Number of documents to send in each bulk insert batch.
        /// <para>Value:</para>
        /// <para>512 by default</para>
        /// </summary>
        /// <value>512 by default</value>
        public int BatchSize { get; set; }

        /// <summary>
        /// Maximum timeout in milliseconds to wait for document write. Exception will be thrown when timeout is elapsed.
        /// <para>Value:</para>
        /// <para>15000 milliseconds by default</para>
        /// </summary>
        /// <value>15000 milliseconds by default</value>
        public int WriteTimeoutMilliseconds { get; set; }

        /// <summary>
        /// Represents options of the chunked functionality of the bulk insert operation, 
        /// which allows opening new connection for each chunk by amount of documents and total size. 
        /// If Set to null, bulk insert will be performed in a not chunked manner.
        /// <para>Value:</para>
        /// <para>Initialize by default</para>
        /// </summary>
        /// <value>Initialized by default</value>
        public ChunkedBulkInsertOptions ChunkedBulkInsertOptions { get; set; }
    }

    /// <summary>
    /// Options for the chunked bulk insert operation
    /// </summary>
    public class ChunkedBulkInsertOptions
    {
        public ChunkedBulkInsertOptions()
        {
            MaxDocumentsPerChunk = 2048;
            MaxChunkVolumeInBytes = 8 * 1024 * 1024;
        }
        /// <summary>
        /// Number of documents to send in each bulk insert sub operation (Default: 2048)
        /// <para>Value:</para>
        /// <para>2048 documents by default</para>
        /// </summary>
        /// <value>2048 documents by default</value>
        public int MaxDocumentsPerChunk { get; set; }

        /// <summary>
        /// Max volume of all the documents could be sent in each bulk insert sub operation (Default: 8MB)
        /// <para>Value:</para>
        /// <para>8MB by default</para>
        /// </summary>
        /// <value>8MB by default</value>
        public long MaxChunkVolumeInBytes { get; set; }
    }
}
