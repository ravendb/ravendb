using System;

namespace Raven.Tests.Bugs.MapRedue
{
	/// <summary>
	/// Wrapper class for versioned documents
	/// </summary>
	public class Document
	{
		/// <summary>
		/// Document Id
		/// </summary>
		public String Id { get; set; }

		/// <summary>
		/// Date when document was "removed" (disabled)
		/// </summary>
		public DateTime? DateRemoved { get; set; }

		/// <summary>
		/// Array containing versions of document
		/// </summary>
		public VersionedDocument[] Versions { get; set; }
	}
}