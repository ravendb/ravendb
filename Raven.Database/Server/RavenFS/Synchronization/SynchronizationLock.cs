using System;
using Raven.Abstractions.RavenFS;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class SynchronizationLock
	{
		public ServerInfo SourceServer { get; set; }
		public DateTime FileLockedAt { get; set; }
	}
}
