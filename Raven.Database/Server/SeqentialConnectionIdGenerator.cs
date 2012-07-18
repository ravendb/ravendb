using System;
using System.Threading;
using Raven.Imports.SignalR;

namespace Raven.Database.Server
{
	public class SeqentialConnectionIdGenerator : IConnectionIdGenerator
	{
		private long counter;

		public string GenerateConnectionId(IRequest request)
		{
			return Interlocked.Increment(ref counter).ToString();
		}
	}
}