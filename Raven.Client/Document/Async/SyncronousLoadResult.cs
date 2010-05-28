using System;
using System.Threading;

namespace Raven.Client.Document.Async
{
	public class SyncronousLoadResult : IAsyncResult
	{
		private readonly object state;
		private readonly object entity;

		public object Entity
		{
			get { return entity; }
		}

		public bool IsCompleted
		{
			get { return true; }
		}

		public WaitHandle AsyncWaitHandle
		{
			get { return null; }
		}

		public object AsyncState
		{
			get { return state; }
		}

		public bool CompletedSynchronously
		{
			get { return true; }
		}

		public SyncronousLoadResult(object state, object entity)
		{
			this.state = state;
			this.entity = entity;
		}
	}
}