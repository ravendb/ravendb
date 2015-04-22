namespace Raven.Abstractions.Data
{
    public enum BulkInsertCompression
    {
        None = 0,
        GZip = 1,        
    }

    public enum BulkInsertFormat
    {
        Bson = 0, 
        Json = 1,        
    }

	/// <summary>
	/// Options used during BulkInsert execution.
	/// </summary>
    public class BulkInsertOptions
	{
		public BulkInsertOptions()
		{
			BatchSize = 512;
			WriteTimeoutMilliseconds = 15 * 1000;
            Compression = BulkInsertCompression.GZip;
            Format = BulkInsertFormat.Bson;
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
        /// This specify which compression format we will use. Some are better than others and/or special purpose. 
        /// You can also disable compression altogether.
        /// </summary>
        /// <remarks>Pre v3.5 bulk inserts only support GZip compression.</remarks>
        public BulkInsertCompression Compression { get; set; }

        /// <summary>
        /// Will specify which type of format you will send the bulk insert request. While the default is most of the
        /// times enough for you. Selecting the proper encoding for bulk inserts based on you data assumptions could give 
        /// your code a performance push and/or smaller network requirements.
        /// </summary>
        /// <remarks>Pre v3.5 bulk inserts only support BSON format.</remarks>
        public BulkInsertFormat Format { get; set; }
	}
}