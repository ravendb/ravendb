//-----------------------------------------------------------------------
// <copyright file="PatchResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
	/// <summary>
	/// The result of a patch operation
	/// </summary>
	public enum PatchResult
	{
		/// <summary>
		/// The document does not exists, operation was a no-op
		/// </summary>
		DocumentDoesNotExists,
		/// <summary>
		/// Document was properly patched
		/// </summary>
		Patched,
		/// <summary>
		/// Document was properly tested
		/// </summary>
		Tested,
		/// <summary>
		/// The document was not patched, because skipPatchIfEtagMismatch was set
		/// and the etag did not match
		/// </summary>
		Skipped,
		/// <summary>
		/// Neither document body not metadata was changed during patch operation
		/// </summary>
		NotModified
	}

	public class PatchResultData
	{
		public PatchResult PatchResult { get; set; }
		public RavenJObject Document { get; set; }
	}
}
