using System.Diagnostics;

namespace Voron.Tests.Bugs
{
	using System;
	using System.IO;
	using System.Threading.Tasks;

	using Voron.Impl;
	using Xunit;

	public class InvalidReleasesOfScratchPages : StorageTest
	{
		[Fact]
		public void ReadTransactionCanReadJustCommittedValue()
		{
			var options = StorageEnvironmentOptions.CreateMemoryOnly();
			options.ManualFlushing = true;
			using (var env = new StorageEnvironment(options))
			{
				CreateTrees(env, 1, "tree");

				using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					txw.Environment.State.GetTree(txw, "tree0").Add(txw, "key/1", new MemoryStream());
					txw.Commit();

					using (var txr = env.NewTransaction(TransactionFlags.Read))
					{
						Assert.NotNull(txr.Environment.State.GetTree(txr, "tree0").Read(txr, "key/1"));
					}
				}
			}
		}

		[Fact]
		public void ParallelWritesInBatchesAndReadsByUsingTreeIterator()
		{
			const int numberOfWriteThreads = 10;
			const int numberOfReadThreads = 10;
			const int numberOfTrees = 2;

			var trees = CreateTrees(Env, numberOfTrees, "tree");

			Task readParallelTask = null;

			var taskWorkTime = TimeSpan.FromSeconds(60);

			var writeTime = Stopwatch.StartNew();

			var writeParallelTask = Task.Factory.StartNew(
				() =>
				{
					Parallel.For(
						0,
						numberOfWriteThreads,
						i =>
						{
							var random = new Random(i ^ 1337);
							var dataSize = random.Next(100, 100);
							var buffer = new byte[dataSize];
							random.NextBytes(buffer);

							while (writeTime.Elapsed < taskWorkTime && (readParallelTask == null || readParallelTask.Exception == null))
							{
								var tIndex = random.Next(0, numberOfTrees - 1);
								var treeName = trees[tIndex];

								var batch = new WriteBatch();
								batch.Add("testdocuments/" + random.Next(0, 100000), new MemoryStream(buffer), treeName);

								Env.Writer.Write(batch);
							}
						});
				},
				TaskCreationOptions.LongRunning);

			var readTime = Stopwatch.StartNew();
			readParallelTask = Task.Factory.StartNew(
				() =>
					{
						Parallel.For(
							0,
							numberOfReadThreads,
							i =>
								{
									var random = new Random(i);

									while (readTime.Elapsed < taskWorkTime)
									{
										var tIndex = random.Next(0, numberOfTrees - 1);
										var treeName = trees[tIndex];

										using (var snapshot = Env.CreateSnapshot())
										using (var iterator = snapshot.Iterate(treeName))
										{
											if (!iterator.Seek(Slice.BeforeAllKeys))
											{
												continue;
											}

											do
											{
												Assert.Contains("testdocuments/", iterator.CurrentKey.ToString());
											} while (iterator.MoveNext());
										}
									}
								});
					},
				TaskCreationOptions.LongRunning);


			try
			{
				Task.WaitAll(new[] { writeParallelTask, readParallelTask });
			}
			catch (Exception ex)
			{
				var aggregate = ex as AggregateException;

				if (aggregate != null)
				{
					foreach (var innerEx in aggregate.InnerExceptions)
					{
						Console.WriteLine(innerEx);
					}
				}

				throw ex;
			}
		}
	}
}