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
	}
}