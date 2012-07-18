using System;
using System.Globalization;
using System.Threading;
using Raven.Imports.SignalR;

namespace Raven.Database.Server
{
	public class SeqentialConnectionIdGenerator : IConnectionIdGenerator
	{
		private static long counter;

		public string GenerateConnectionId(IRequest request)
		{
			// Ensure the connection id is unique, but at the same time, has a 
			// part that is human readable
			return Interlocked.Increment(ref counter).ToString("#,#", CultureInfo.InvariantCulture) + "@" + Guid.NewGuid("d");
		}
	}
}