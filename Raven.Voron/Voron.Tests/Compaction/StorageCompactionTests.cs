// -----------------------------------------------------------------------
//  <copyright file="StorageCompactionTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;
using Voron.Impl.Compaction;

namespace Voron.Tests.Compaction
{
	public class StorageCompactionTests : IDisposable
	{
		public const string CompactionTestsData = "Data";
		public const string CompactedData = "Data.Compacted";

		public StorageCompactionTests()
		{
			ClearDirs();
		}

		[Fact]
		public void CompactionMustNotLooseAnyData()
		{
			var treeNames = new List<string>();
			var multiValueTreeNames = new List<string>();

			var random = new Random();

			var value1 = new byte[random.Next(1024*1024*2)];
			var value2 = new byte[random.Next(1024*1024*2)];

			random.NextBytes(value1);
			random.NextBytes(value2);

			const int treeCount = 5;
			const int recordCount = 6;
			const int multiValueTreeCount = 7;
			const int multiValueRecordsCount = 4;
			const int multiValuesCount = 3;

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(CompactionTestsData)))
			{
				for (int i = 0; i < treeCount; i++)
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						string name = "tree/" + i;
						treeNames.Add(name);

						var tree = env.State.GetTree(tx, name);

						for (int j = 0; j < recordCount; j++)
						{
							tree.Add(string.Format("{0}/items/{1}", name, j), j%2 == 0 ? value1 : value2);
						}

						tx.Commit();
					}
				}

				for (int i = 0; i < multiValueTreeCount; i++)
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						var name = "multiValueTree/" + i;
						multiValueTreeNames.Add(name);

						var tree = env.CreateTree(tx, name);

						for (int j = 0; j < multiValueRecordsCount; j++)
						{
							for (int k = 0; k < multiValuesCount; k++)
							{
								tree.MultiAdd("record/" + j, "value/" + k);
							}
						}

						tx.Commit();
					}
				}
			}

			StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(CompactionTestsData), StorageEnvironmentOptions.ForPath(CompactedData), maxTransactionSizeInBytes: 256);

			using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPath(CompactedData)))
			{
				using (var tx = compacted.NewTransaction(TransactionFlags.Read))
				{
					foreach (var treeName in treeNames)
					{
						var tree = compacted.State.GetTree(tx, treeName);

						for (int i = 0; i < recordCount; i++)
						{
							var readResult = tree.Read(string.Format("{0}/items/{1}", treeName, i));

							Assert.NotNull(readResult);

							if (i%2 == 0)
							{
								var readBytes = new byte[value1.Length];
								readResult.Reader.Read(readBytes, 0, readBytes.Length);

								Assert.Equal(value1, readBytes);
							}
							else
							{
								var readBytes = new byte[value2.Length];
								readResult.Reader.Read(readBytes, 0, readBytes.Length);

								Assert.Equal(value2, readBytes);
							}
						}
					}

					foreach (var treeName in multiValueTreeNames)
					{
						var tree = compacted.State.GetTree(tx, treeName);

						for (int i = 0; i < multiValueRecordsCount; i++)
						{
							var multiRead = tree.MultiRead("record/" + i);

							Assert.True(multiRead.Seek(Slice.BeforeAllKeys));

							int count = 0;
							do
							{
								Assert.Equal("value/" + count, multiRead.CurrentKey.ToString());
								count++;
							} while (multiRead.MoveNext());

							Assert.Equal(multiValuesCount, count);
						}
					}
				}
			}
		}

		[Fact]
		public void AfterCompactionStorageShouldTakeLessSpace()
		{
			var r = new Random();
			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(CompactionTestsData)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = env.CreateTree(tx, "records");

					for (int i = 0; i < 100; i++)
					{
						var bytes = new byte[r.Next(10, 2*1024*1024)];
						r.NextBytes(bytes);

						tree.Add("record/" + i, bytes);
					}

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = env.CreateTree(tx, "records");

					for (int i = 0; i < 50; i++)
					{
						tree.Delete("record/" + r.Next(0, 100));
					}

					tx.Commit();
				}
			}

			var oldSize = DirectorySize(new DirectoryInfo(CompactionTestsData));

			StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(CompactionTestsData), StorageEnvironmentOptions.ForPath(CompactedData), maxTransactionSizeInBytes: 256);

			var newSize = DirectorySize(new DirectoryInfo(CompactedData));

			Assert.True(newSize < oldSize);
		}

		public static long DirectorySize(DirectoryInfo d)
		{
			var files = d.GetFiles();
			var size = files.Sum(fi => fi.Length);

			var dis = d.GetDirectories();
			size += dis.Sum(di => DirectorySize(di));

			return size;
		}

		public void Dispose()
		{
			ClearDirs();
		}

		private static void ClearDirs()
		{
			if (Directory.Exists(CompactionTestsData))
				StorageTest.DeleteDirectory(CompactionTestsData);

			if (Directory.Exists(CompactedData))
				StorageTest.DeleteDirectory(CompactedData);
		}
	}
}