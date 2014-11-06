using System;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.FileSystem.Synchronization
{
	public class SynchronizationLock
	{
		public ServerInfo SourceServer { get; set; }
		public DateTime FileLockedAt { get; set; }
	}
}
