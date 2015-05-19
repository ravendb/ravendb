using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Database.Util;

namespace Raven.Database.Server.Connections
{
	public class FileSystemConnectionState
	{
		private readonly Action<object> enqueue;

		private readonly ConcurrentSet<string> matchingFolders =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private int watchConflicts;
		private int watchSync;
		private int watchCancellations;
		private int watchConfig;

		public FileSystemConnectionState(Action<object> enqueue)
		{
			this.enqueue = enqueue;
		}

		public object DebugStatus
		{
			get
			{
				return new
				{
					WatchConflicts = watchConflicts > 0,
					WatchSync = watchSync > 0,
					WatchCancellations = watchCancellations > 0,
					WatchConfig = watchConfig > 0,
					WatchedFolders = matchingFolders.ToArray()
				};
			}
		}

		public void WatchConflicts()
		{
			Interlocked.Increment(ref watchConflicts);
		}

		public void UnwatchConflicts()
		{
			Interlocked.Decrement(ref watchConflicts);
		}

		public void WatchSync()
		{
			Interlocked.Increment(ref watchSync);
		}

		public void UnwatchSync()
		{
			Interlocked.Decrement(ref watchSync);
		}

		public void WatchFolder(string folder)
		{
			matchingFolders.TryAdd(folder);
		}

		public void UnwatchFolder(string folder)
		{
			matchingFolders.TryRemove(folder);
		}

		public void WatchCancellations()
		{
			Interlocked.Increment(ref watchCancellations);
		}

		public void UnwatchCancellations()
		{
			Interlocked.Decrement(ref watchCancellations);
		}

		public void WatchConfig()
		{
			Interlocked.Increment(ref watchConfig);
		}

		public void UnwatchConfig()
		{
			Interlocked.Decrement(ref watchConfig);
		}

		public void Send(FileSystemNotification fileSystemNotification)
		{
			if (ShouldSend(fileSystemNotification))
			{
				var value = new { Value = fileSystemNotification, Type = fileSystemNotification.GetType().Name };

				enqueue(value);
			}
		}

		private bool ShouldSend(FileSystemNotification fileSystemNotification)
		{
			if (fileSystemNotification is FileChangeNotification &&
			    matchingFolders.Any(
				    f => ((FileChangeNotification)fileSystemNotification).File.StartsWith(f, StringComparison.InvariantCultureIgnoreCase)))
			{
				return true;
			}

			if (fileSystemNotification is ConfigurationChangeNotification && watchConfig > 0)
			{
				return true;
			}

			if (fileSystemNotification is ConflictNotification && watchConflicts > 0)
			{
				return true;
			}

			if (fileSystemNotification is SynchronizationUpdateNotification && watchSync > 0)
			{
				return true;
			}

			return false;
		}
	}
}