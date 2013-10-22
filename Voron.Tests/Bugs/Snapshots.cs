namespace Voron.Tests.Bugs
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Text;
	using System.Threading.Tasks;

	using Voron.Impl;
	using Voron.Trees;

	using Xunit;

	public class Snapshots : StorageTest
	{
		[Fact]
		public void CreatingSnapshotsDuringTransactionCommitShouldWork()
		{
			using (var env = new StorageEnvironment(new PureMemoryPager()))
			{
				var rand = new Random();
				var testBuffer = new byte[79];
				rand.NextBytes(testBuffer);

				var trees1 = CreateTrees(env, 10000, "tree_1_");
				var trees2 = CreateTrees(env, 10000, "tree_2_");

				var commited = false;

				var t1 = Task.Run(
					() =>
					{
						while (commited == false)
						{
							Validate(env, trees1, trees2, 0);
						}
					});

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					foreach (var tree in trees1)
					{
						tree.Add(tx, Guid.NewGuid().ToString(), new MemoryStream(Encoding.UTF8.GetBytes("docs/1")));
					}

					foreach (var tree in trees2)
					{
						tree.Add(tx, "docs/1", new MemoryStream(testBuffer));
					}

					tx.Commit();

					commited = true;
				}

				Task.WaitAll(t1);
				
				Validate(env, trees1, trees2, 1);
			}
		}

		private void Validate(StorageEnvironment env, IList<Tree> trees1, IList<Tree> trees2, int numberOfRecords)
		{
			using (var snapshot = env.CreateSnapshot())
			{
				foreach (var tree in trees1)
				{
					using (var iterator = snapshot.Iterate(tree.Name))
					{
						var seek = iterator.Seek(Slice.BeforeAllKeys);
						if (seek == false && numberOfRecords > 0)
							Assert.True(false, "No records found, but we expect: " + numberOfRecords);

						if (seek && numberOfRecords == 0)
							Assert.True(false, "Records found, but we expect none");

						if (!seek && numberOfRecords == 0)
							return;

						var keys = new HashSet<string>();

						var count = 0;
						do
						{
							keys.Add(iterator.CurrentKey.ToString());
							Assert.NotNull(snapshot.Read(tree.Name, iterator.CurrentKey));

							var k = GetKeyFromCurrent(iterator);
							Assert.True(k == "docs/1");

							foreach (var tree2 in trees2)
							{
								Assert.NotNull(snapshot.Read(tree2.Name, k));
							}

							count++;
						}
						while (iterator.MoveNext());

						Assert.Equal(numberOfRecords, count);
					}
				}
			}
		}

		private static string GetKeyFromCurrent(IIterator iterator)
		{
			string key;
			using (var currentDataStream = iterator.CreateStreamForCurrent())
			{
				var keyBytes = currentDataStream.ReadData();
				key = Encoding.UTF8.GetString(keyBytes);
			}
			return key;
		}

		[Fact]
		public void SnapshotIssue()
		{
			const int DocumentCount = 50000;

			var rand = new Random();
			var testBuffer = new byte[39];
			rand.NextBytes(testBuffer);

			Tree t1;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				t1 = Env.CreateTree(tx, "tree1");
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < DocumentCount; i++)
				{
					t1.Add(tx, "docs/" + i, new MemoryStream(testBuffer));
				}

				tx.Commit();
			}

			using (var snapshot = Env.CreateSnapshot())
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < DocumentCount; i++)
					{
						t1.Delete(tx, "docs/" + i);
					}

					tx.Commit();
				}

				for (var i = 0; i < DocumentCount; i++)
				{
					var result = snapshot.Read(t1.Name, "docs/" + i);
					Assert.NotNull(result);

					using (var reader = new BinaryReader(result.Stream))
					{
						Assert.Equal(testBuffer, reader.ReadBytes((int)result.Stream.Length));
					}
				}
			}
		}
	}
}