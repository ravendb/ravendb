using System;
using System.Collections.Generic;

namespace Raven.Client.Connection.Profiling
{
	/// <summary>
	/// Information about a particular session
	/// </summary>
#if !SILVERLIGHT
	[Serializable]
#endif
	public class ProfilingInformation
	{
		///<summary>
		/// The requests made by this session
		///</summary>
		public List<RequestResultArgs> Requests = new List<RequestResultArgs>();

		/// <summary>
		/// Uniquely identify the session
		/// </summary>
		public Guid Id;

		/// <summary>
		/// The time when the session was created
		/// </summary>
		public DateTime At = DateTime.Now;

		///<summary>
		/// Create a new instance of this class
		///</summary>
		public ProfilingInformation(Guid? sessionId)
		{
			Id = sessionId ?? Guid.NewGuid();
		}
	}
}