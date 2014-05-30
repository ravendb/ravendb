using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Storage;
using Xunit;

namespace RavenFS.Tests.Synchronization
{
	public class SyncTestUtils
	{
		public static SynchronizationReport ResolveConflictAndSynchronize(RavenFileSystemClient sourceClient,
		                                                                  RavenFileSystemClient destinationClient,
		                                                                  string fileName)
		{
			var shouldBeConflict = sourceClient.Synchronization.StartAsync(fileName, destinationClient).Result;

			Assert.NotNull(shouldBeConflict.Exception);

			destinationClient.Synchronization.ResolveConflictAsync(fileName, ConflictResolutionStrategy.RemoteVersion).Wait();
			return sourceClient.Synchronization.StartAsync(fileName, destinationClient).Result;
		}

		public static void TurnOnSynchronization(RavenFileSystemClient source, params RavenFileSystemClient[] destinations)
		{
            source.Config.SetDestinationsConfig(destinations.Select(x => new SynchronizationDestination()
            {
                FileSystem = x.FileSystemName,
                ServerUrl = x.ServerUrl
            }).ToArray()).Wait();
		}

		public static void TurnOffSynchronization(RavenFileSystemClient source)
		{
			source.Config.DeleteConfig(SynchronizationConstants.RavenSynchronizationDestinations).Wait();
		}

		public static Exception ExecuteAndGetInnerException(Func<Task> action)
		{
			Exception innerException = null;

			try
			{
				action().Wait();
			}
			catch (AggregateException exception)
			{
				innerException = exception.InnerException;
			}

			return innerException;
		}

		public static MemoryStream PrepareSourceStream(int lines)
		{
			var ms = new MemoryStream();
			var writer = new StreamWriter(ms);

			for (var i = 1; i <= lines; i++)
			{
				for (var j = 0; j < 100; j++)
				{
					writer.Write(i.ToString("D4"));
				}
				writer.Write("\n");
			}
			writer.Flush();

			return ms;
		}

		public static MemoryStream PreparePagesStream(int numberOfPages)
		{
			var ms = new MemoryStream();
			var random = new Random();

			for (var i = 1; i <= numberOfPages; i++)
			{
				var page = new byte[StorageConstants.MaxPageSize];
				random.NextBytes(page);
				ms.Write(page, 0, StorageConstants.MaxPageSize);
			}

			return ms;
		}
	}
}