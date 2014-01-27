using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Json.Linq;
using Rhino.Mocks;
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

		[Fact]
		public void AsyncGeneration_RetryOnConcurencyEx()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var calls = 0;

				var mockedDbCommands = MockRepository.GenerateMock<IAsyncDatabaseCommands>();
				mockedDbCommands.Stub(c => c.GetAsync(Arg<string[]>.Is.Anything, Arg<string[]>.Is.Anything, Arg<string>.Is.Anything, Arg<Dictionary<string, RavenJToken>>.Is.Anything, Arg<bool>.Is.Anything))
								.Return(null)
								.WhenCalled(m => { m.ReturnValue = store.AsyncDatabaseCommands.GetAsync((string[])m.Arguments[0], (string[])m.Arguments[1], (string)m.Arguments[2], (Dictionary<string, RavenJToken>)m.Arguments[3], (bool)m.Arguments[4]); });

				mockedDbCommands.Stub(c => c.PutAsync(Arg<string>.Is.Anything, Arg<Raven.Abstractions.Data.Etag>.Is.Anything, Arg<RavenJObject>.Is.Anything, Arg<RavenJObject>.Is.Anything))
								.Return(null)
								.WhenCalled(
									m =>
									{
										calls++;
										if (calls == 1)
										{
											var taskSource = new TaskCompletionSource<PutResult>();
											taskSource.SetException(new ConcurrencyException());
											m.ReturnValue = taskSource.Task;
										}
										else
										{
											m.ReturnValue = store.AsyncDatabaseCommands.PutAsync((string)m.Arguments[0], (Raven.Abstractions.Data.Etag)m.Arguments[1], (RavenJObject)m.Arguments[2], (RavenJObject)m.Arguments[3]);
										}

									});

				var gen = new AsyncHiLoKeyGenerator("Async retries on ConcurencyException", 2);
				gen.NextIdAsync(mockedDbCommands).Wait();

				Assert.Equal(2, calls);
			}
		}

		[Fact]
		public void AsyncGeneration_RetryOnConcurencyExWithConflicts()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var document = new JsonDocument
				{
					Etag = Guid.Empty,
					Metadata = new RavenJObject(),
					DataAsJson = RavenJObject.FromObject(new { Max = 0 }),
					Key = "Raven/Hilo/SomeDocument"
				};

				// Create a document that will be used to resolve the conflicts.
				store.AsyncDatabaseCommands.PutAsync(document.Key, document.Etag, document.DataAsJson, document.Metadata).Wait();

				var callsToGetAsync = 0;
				var callsToPutAsync = 0;

				var mockedDbCommands = MockRepository.GenerateMock<IAsyncDatabaseCommands>();

				mockedDbCommands.Stub(c => c.GetAsync(Arg<string>.Is.Anything))
								.Return(null)
								.WhenCalled(m => { m.ReturnValue = store.AsyncDatabaseCommands.GetAsync((string)m.Arguments[0]); });

				mockedDbCommands.Stub(c => c.GetAsync(Arg<string[]>.Is.Anything, Arg<string[]>.Is.Anything, Arg<string>.Is.Anything, Arg<Dictionary<string, RavenJToken>>.Is.Anything, Arg<bool>.Is.Anything))
								.Return(null)
								.WhenCalled(m =>
								{
									callsToGetAsync++;
									if (callsToGetAsync == 1)
									{
										var taskSource = new TaskCompletionSource<MultiLoadResult>();
										taskSource.SetException(new ConflictException(false)
										{
											ConflictedVersionIds = new[] { document.Key },
											Etag = Guid.Empty
										});
										m.ReturnValue = taskSource.Task;
									}
									else
									{
										m.ReturnValue = store.AsyncDatabaseCommands.GetAsync((string[])m.Arguments[0], (string[])m.Arguments[1], (string)m.Arguments[2], (Dictionary<string, RavenJToken>)m.Arguments[3], (bool)m.Arguments[4]);
									}
								});

				mockedDbCommands.Stub(c => c.PutAsync(Arg<string>.Is.Anything, Arg<Raven.Abstractions.Data.Etag>.Is.Anything, Arg<RavenJObject>.Is.Anything, Arg<RavenJObject>.Is.Anything))
								.Return(null)
								.WhenCalled(
									m =>
									{
										callsToPutAsync++;
										if (callsToPutAsync == 1)
										{
											var taskSource = new TaskCompletionSource<PutResult>();
											taskSource.SetException(new ConcurrencyException());
											m.ReturnValue = taskSource.Task;
										}
										else
										{
											m.ReturnValue = store.AsyncDatabaseCommands.PutAsync((string)m.Arguments[0], (Raven.Abstractions.Data.Etag)m.Arguments[1], (RavenJObject)m.Arguments[2], (RavenJObject)m.Arguments[3]);
										}

									});

				var gen = new AsyncHiLoKeyGenerator("Async retries on ConcurencyException", 2);
				gen.NextIdAsync(mockedDbCommands).Wait();

				Assert.Equal(2, callsToGetAsync);
				Assert.Equal(2, callsToPutAsync);
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
