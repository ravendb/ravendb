using System;
using System.Threading;

namespace Raven.Client.Document.Async
{
	/// <summary>
	/// An <see cref="IAsyncResult"/> for a synchronous load
	/// </summary>
	public class SyncronousLoadResult : IAsyncResult
	{
		private readonly object state;
		private readonly object entity;

		/// <summary>
		/// Gets the entity.
		/// </summary>
		/// <value>The entity.</value>
		public object Entity
		{
			get { return entity; }
		}

		/// <summary>
		/// Gets a value that indicates whether the asynchronous operation has completed.
		/// </summary>
		/// <value></value>
		/// <returns>true if the operation is complete; otherwise, false.</returns>
		public bool IsCompleted
		{
			get { return true; }
		}

		/// <summary>
		/// Gets a <see cref="T:System.Threading.WaitHandle"/> that is used to wait for an asynchronous operation to complete.
		/// </summary>
		/// <value></value>
		/// <returns>A <see cref="T:System.Threading.WaitHandle"/> that is used to wait for an asynchronous operation to complete.</returns>
		public WaitHandle AsyncWaitHandle
		{
			get { return null; }
		}

		/// <summary>
		/// Gets a user-defined object that qualifies or contains information about an asynchronous operation.
		/// </summary>
		/// <value></value>
		/// <returns>A user-defined object that qualifies or contains information about an asynchronous operation.</returns>
		public object AsyncState
		{
			get { return state; }
		}

		/// <summary>
		/// Gets a value that indicates whether the asynchronous operation completed synchronously.
		/// </summary>
		/// <value></value>
		/// <returns>true if the asynchronous operation completed synchronously; otherwise, false.</returns>
		public bool CompletedSynchronously
		{
			get { return true; }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SyncronousLoadResult"/> class.
		/// </summary>
		/// <param name="state">The state.</param>
		/// <param name="entity">The entity.</param>
		public SyncronousLoadResult(object state, object entity)
		{
			this.state = state;
			this.entity = entity;
		}
	}
}
