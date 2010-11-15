namespace Raven.Database
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
		Patched
	}
}
