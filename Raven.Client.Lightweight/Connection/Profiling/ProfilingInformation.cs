using System;
using System.Collections.Generic;

namespace Raven.Client.Connection.Profiling
{
	/// <summary>
	/// Information about a particular session
	/// </summary>
	public class ProfilingInformation
	{
		///<summary>
		/// The requests made by this session
		///</summary>
		public List<RequestResultArgs> Requests = new List<RequestResultArgs>();

		/// <summary>
		/// Uniquely identify the session
		/// </summary>
		public Guid Id = Guid.NewGuid();

		/// <summary>
		/// The time when the session was created
		/// </summary>
		public DateTime At = DateTime.Now;
	}
}