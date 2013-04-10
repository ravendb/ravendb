using System;

namespace Raven.Tests.Bugs.MapRedue
{
	/// <summary>
	/// View for index VersionedDocuments
	/// </summary>
	public class DocumentView
	{
		/// <summary>
		/// Document id
		/// </summary>
		public String Id { get; set; }

		/// <summary>
		/// Version number of document
		/// </summary>
		[CLSCompliant(false)]
		public uint Version { get; set; }

		/// <summary>
		/// Instace of versioned document with the same version number
		/// </summary>
		public VersionedDocument Document { get; set; }
	}
}