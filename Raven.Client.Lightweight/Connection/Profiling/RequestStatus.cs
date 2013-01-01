namespace Raven.Client.Connection.Profiling
{
	/// <summary>
	/// The status of the request
	/// </summary>
	public enum RequestStatus
	{
		/// <summary>
		/// The request was sent to the server
		/// </summary>
		SentToServer,
		/// <summary>
		/// The request was served directly from the local cache
		/// after checking with the server to see if it was still 
		/// up to date
		/// </summary>
		Cached,
		/// <summary>
		/// The request was served from the local cache without
		/// checking with the server and may be out of date
		/// </summary>
		AggressivelyCached,
		/// <summary>
		/// The server returned an error
		/// </summary>
		ErrorOnServer,
	}
}