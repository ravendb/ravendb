using System;
using System.Collections.Generic;
using Raven.Abstractions;

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
		/// A hook that allows extensions to provide additional information about the created context
		///</summary>
		public static event Action<ProfilingInformation> OnContextCreated = delegate { }; 

		///<summary>
		/// Create a new instance of profiling information and provide additional context information
		///</summary>
		public static ProfilingInformation CreateProfilingInformation(Guid? sessionId)
		{
			var profilingInformation = new ProfilingInformation(sessionId);
			OnContextCreated(profilingInformation);
			return profilingInformation;
		}

		/// <summary>
		/// Additional information that is added by extension
		/// </summary>
		public IDictionary<string, string> Context { get; set; }

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
		public DateTime At = SystemTime.UtcNow;

		/// <summary>
		/// The duration this session was opened
		/// </summary>
		public double DurationMilliseconds;

		///<summary>
		/// Create a new instance of this class
		///</summary>
		private ProfilingInformation(Guid? sessionId)
		{
			Id = sessionId ?? Guid.NewGuid();
			Context = new Dictionary<string, string>();
		}
	}
}