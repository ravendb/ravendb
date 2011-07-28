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
		/// Date when documnet was "removed" (disabled)
		/// </summary>
		public DateTime? DateRemoved { get; set; }

		/// <summary>
		/// Array containnig versions of document
		/// </summary>
		public VersionedDocument[] Versions { get; set; }
	}
}