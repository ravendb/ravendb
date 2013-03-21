using System;

namespace Raven.Tests.Bugs.MapRedue
{
	/// <summary>
	/// Document with ID and version number
	/// </summary>
	public class VersionedDocument
	{
		/// <summary>
		/// Document id
		/// </summary>
		public string Id { get; set; }

		/// <summary>
		/// Version number of document
		/// </summary>
		[CLSCompliant(false)]
		public uint Version { get; set; }

		/// <summary>
		/// Some data included in document
		/// </summary>
		public string Data { get; set; }
	}
}