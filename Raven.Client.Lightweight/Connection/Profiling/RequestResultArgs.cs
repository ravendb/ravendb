using System;

namespace Raven.Client.Connection.Profiling
{
	/// <summary>
	/// The result of a request made to the server
	/// </summary>
#if !SILVERLIGHT
	[Serializable]
#endif
	public class RequestResultArgs : EventArgs
	{
		/// <summary>
		/// Creates a new instance of <seealso cref="RequestResultArgs"/>
		/// </summary>
		public RequestResultArgs()
		{
			At = DateTime.Now;
		}

		/// <summary>
		/// When the request completed
		/// </summary>
		public DateTime At { get; set; }
		/// <summary>
		/// The request status
		/// </summary>
		public RequestStatus Status { get; set; }
		/// <summary>
		/// The request Url
		/// </summary>
		public string Url { get; set; }
		/// <summary>
		/// The request method
		/// </summary>
		public string Method { get; set; }
		/// <summary>
		/// The data posted to the server
		/// </summary>
		public string PostedData { get; set; }
		/// <summary>
		/// The HTTP result for this request
		/// </summary>
		public int HttpResult { get; set; }
		/// <summary>
		/// The result of this request
		/// </summary>
		public string Result { get; set; }
	}
}