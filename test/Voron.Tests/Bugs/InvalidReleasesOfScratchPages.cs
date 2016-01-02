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

				using (var txw = env.WriteTransaction())
				{
					txw.CreateTree("tree0").Add("key/1", new MemoryStream());
					txw.Commit();

					using (var txr = env.ReadTransaction())
					{
						Assert.NotNull(txr.CreateTree("tree0").Read("key/1"));
					}
				}
			}
		}

		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxScratchBufferSize *= 2;
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

							    using (var tx = Env.WriteTransaction())
							    {
							        var tree = tx.CreateTree(treeName);
							        tree.Add("testdocuments/" + random.Next(0, 100000), new MemoryStream(buffer));
                                    tx.Commit();
							    }

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

										using (var snapshot = Env.ReadTransaction())
										using (var iterator = snapshot.ReadTree(treeName).Iterate())
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

		[Fact]
		public void AllScratchPagesShouldBeReleased()
		{
			var options = StorageEnvironmentOptions.CreateMemoryOnly();
			options.ManualFlushing = true;
			using (var env = new StorageEnvironment(options))
			{
				using (var txw = env.WriteTransaction())
				{
					txw.CreateTree("test");

					txw.Commit();
				}

				using (var txw = env.WriteTransaction())
				{
					var tree = txw.CreateTree("test");

					tree.Add("key/1", new MemoryStream(new byte[100]));
					tree.Add("key/1", new MemoryStream(new byte[200]));
					txw.Commit();
				}

				env.FlushLogToDataFile(); // non read nor write transactions, so it should flush and release everything from scratch

				Assert.Equal(0, env.ScratchBufferPool.GetNumberOfAllocations(0));
			}
		}
	}
}