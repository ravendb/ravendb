using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;
using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Bugs
{
	public class HiLoKeyGeneratorConcurrency : RemoteClientTest
	{
		private const int GeneratedIdCount = 2000;
		private const int ThreadCount = 100;

		[Fact]
		public void ParallelGeneration_NoClashesOrGaps()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var gen = new HiLoKeyGenerator("When_generating_lots_of_keys_concurrently_there_are_no_clashes", 2);
				Test(() => gen.NextId(store.DatabaseCommands), ThreadCount, GeneratedIdCount);
			}
		}

		[Fact]
		public void AsyncParallelGeneration_NoClashesOrGaps()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var gen = new AsyncHiLoKeyGenerator("When_async_generating_lots_of_keys_concurrently_there_are_no_clashes", 2);
				Test(() => gen.NextIdAsync(store.AsyncDatabaseCommands).Result, ThreadCount, GeneratedIdCount);
			}
		}

		[Fact]
		public void SequentialGeneration_NoClashesOrGaps()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var gen = new HiLoKeyGenerator("When_generating_lots_of_keys_concurrently_there_are_no_clashes", 2);
				Test(() => gen.NextId(store.DatabaseCommands), 1, GeneratedIdCount);
			}
		}

		[Fact]
		public void AsyncSequentialGeneration_NoClashesOrGaps()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var gen = new AsyncHiLoKeyGenerator("When_async_generating_lots_of_keys_concurrently_there_are_no_clashes", 2);
				Test(() => gen.NextIdAsync(store.AsyncDatabaseCommands).Result, 1, GeneratedIdCount);
			}
		}

		private void Test(Func<long> generate, int threadCount, int generatedIdCount)
		{
			var waitingThreadCount = 0;
			var starterGun = new ManualResetEvent(false);

			var results = new long[generatedIdCount];
			var threads = Enumerable.Range(0, threadCount).Select(threadNumber => new Thread(() =>
			{
				// Wait for all threads to be ready
				Interlocked.Increment(ref waitingThreadCount);
				starterGun.WaitOne();

				for (int i = threadNumber; i < generatedIdCount; i += threadCount)
					results[i] = generate();
			})).ToArray();

			foreach (var t in threads)
				t.Start();

			// Wait for all tasks to reach the waiting stage
			var wait = new SpinWait();
			while (waitingThreadCount < threadCount)
				wait.SpinOnce();

			// Start all the threads at the same time
			starterGun.Set();
			foreach (var t in threads)
				t.Join();

			var ids = new HashSet<long>();
			foreach (var value in results)
			{
				if (!ids.Add(value))
				{
					throw new AssertException("Id " + value + " was generated more than once, in indices "
						+ string.Join(", ", results.Select(Tuple.Create<long, int>).Where(x => x.Item1 == value).Select(x => x.Item2)));
				}
			}

			for (long i = 1; i <= GeneratedIdCount; i++)
				Assert.True(ids.Contains(i), "Id " + i + " was not generated.");
		}
	}
}
