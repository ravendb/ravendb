using System;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.FileSystem.Synchronization
{
	public class SynchronizationLock
	{
		public FileSystemInfo SourceFileSystem { get; set; }
		public DateTime FileLockedAt { get; set; }
	}
}
