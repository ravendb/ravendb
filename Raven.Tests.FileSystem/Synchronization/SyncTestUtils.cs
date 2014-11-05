using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Database.FileSystem.Storage;
using Xunit;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Extensions;

namespace Raven.Tests.FileSystem.Synchronization
{
	public class SyncTestUtils
	{
        public static SynchronizationReport ResolveConflictAndSynchronize(IAsyncFilesCommands sourceClient,
                                                                          IAsyncFilesCommands destinationClient,
		                                                                  string fileName)
		{
			var shouldBeConflict = sourceClient.Synchronization.StartAsync(fileName, destinationClient).Result;

			Assert.NotNull(shouldBeConflict.Exception);

			destinationClient.Synchronization.ResolveConflictAsync(fileName, ConflictResolutionStrategy.RemoteVersion).Wait();
			return sourceClient.Synchronization.StartAsync(fileName, destinationClient).Result;
		}

        public static void TurnOnSynchronization(IAsyncFilesCommands source, params IAsyncFilesCommands[] destinations)
		{
            source.Synchronization.SetDestinationsAsync(destinations.Select(x => x.ToSynchronizationDestination()).ToArray()).Wait();
		}

		public static void TurnOffSynchronization(IAsyncFilesCommands source)
		{
            var destinations = source.Synchronization.GetDestinationsAsync().Result;
            foreach ( var destination in destinations )
                destination.Enabled = false;

            source.Synchronization.SetDestinationsAsync(destinations).Wait();          
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