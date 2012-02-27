using System;

namespace Raven.Client
{
	/// <summary>
	/// Provide a way for interested party to tell whatever implementers have been disposed
	/// </summary>
	public interface IDisposalNotification : IDisposable
	{
		/// <summary>
		/// Called after dispose is completed
		/// </summary>
		event EventHandler AfterDispose;

		/// <summary>
		/// Whatever the instance has been disposed
		/// </summary>
		bool WasDisposed { get; }
	}
}