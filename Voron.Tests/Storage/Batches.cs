namespace Voron.Tests.Storage
{
	using System;
	using System.Globalization;
	using System.IO;
	using System.Text;
	using System.Threading.Tasks;

	using Voron.Impl;
	using Voron.Trees;

	using Xunit;

	public class Batches : StorageTest
	{
		[Fact]
		public async Task SingleItemBatchTest()
		{
			var batch = new WriteBatch();
			batch.Add("key/1", new MemoryStream(Encoding.UTF8.GetBytes("123")), null);

			await Env.Writer.WriteAsync(batch);

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				using (var stream = Env.Root.Read(tx, "key/1").Stream)
				using (var reader = new StreamReader(stream))
				{
					var result = reader.ReadToEnd();
					Assert.Equal("123", result);
				}
			}
		}

		[Fact]
		public async Task MultipleItemBatchTest()
		{
			int numberOfItems = 10000;

			var batch = new WriteBatch();
			for (int i = 0; i < numberOfItems; i++)
			{
				batch.Add("key/" + i, new MemoryStream(Encoding.UTF8.GetBytes(i.ToString(CultureInfo.InvariantCulture))), null);
			}

			await Env.Writer.WriteAsync(batch);

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				for (int i = 0; i < numberOfItems; i++)
				{
					using (var stream = Env.Root.Read(tx, "key/" + i).Stream)
					using (var reader = new StreamReader(stream))
					{
						var result = reader.ReadToEnd();
						Assert.Equal(i.ToString(CultureInfo.InvariantCulture), result);
					}
				}
			}
		}

		[Fact]
		public async Task MultipleBatchesTest()
		{
			int numberOfItems = 10000;

			var batch1 = new WriteBatch();
			var batch2 = new WriteBatch();
			for (int i = 0; i < numberOfItems; i++)
			{
				batch1.Add("key/" + i, new MemoryStream(Encoding.UTF8.GetBytes(i.ToString(CultureInfo.InvariantCulture))), null);
				batch2.Add("yek/" + i, new MemoryStream(Encoding.UTF8.GetBytes(i.ToString(CultureInfo.InvariantCulture))), null);
			}

			await Task.WhenAll(Env.Writer.WriteAsync(batch1), Env.Writer.WriteAsync(batch2));

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				for (int i = 0; i < numberOfItems; i++)
				{
					using (var stream = Env.Root.Read(tx, "key/" + i).Stream)
					using (var reader = new StreamReader(stream))
					{
						var result = reader.ReadToEnd();
						Assert.Equal(i.ToString(CultureInfo.InvariantCulture), result);
					}

					using (var stream = Env.Root.Read(tx, "yek/" + i).Stream)
					using (var reader = new StreamReader(stream))
					{
						var result = reader.ReadToEnd();
						Assert.Equal(i.ToString(CultureInfo.InvariantCulture), result);
					}
				}
			}
		}

		[Fact]
		public async Task MultipleTreesTest()
		{
			int numberOfItems = 10000;

			var batch1 = new WriteBatch();
			var batch2 = new WriteBatch();
			for (int i = 0; i < numberOfItems; i++)
			{
				batch1.Add("key/" + i, new MemoryStream(Encoding.UTF8.GetBytes(i.ToString(CultureInfo.InvariantCulture))), "tree1");
				batch2.Add("yek/" + i, new MemoryStream(Encoding.UTF8.GetBytes(i.ToString(CultureInfo.InvariantCulture))), "tree2");
			}

			Tree t1;
			Tree t2;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				t1 = Env.CreateTree(tx, "tree1");
				t2 = Env.CreateTree(tx, "tree2");

				tx.Commit();
			}

			await Task.WhenAll(Env.Writer.WriteAsync(batch1), Env.Writer.WriteAsync(batch2));

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				for (int i = 0; i < numberOfItems; i++)
				{
					using (var stream = t1.Read(tx, "key/" + i).Stream)
					using (var reader = new StreamReader(stream))
					{
						var result = reader.ReadToEnd();
						Assert.Equal(i.ToString(CultureInfo.InvariantCulture), result);
					}

					using (var stream = t2.Read(tx, "yek/" + i).Stream)
					using (var reader = new StreamReader(stream))
					{
						var result = reader.ReadToEnd();
						Assert.Equal(i.ToString(CultureInfo.InvariantCulture), result);
					}
				}
			}
		}

		[Fact]
		public async Task MultipleTreesInSingleBatch()
		{
			var batch = new WriteBatch();
			batch.Add("key/1", new MemoryStream(Encoding.UTF8.GetBytes("tree1")), "tree1");
			batch.Add("key/1", new MemoryStream(Encoding.UTF8.GetBytes("tree2")), "tree2");

			Tree t1;
			Tree t2;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				t1 = Env.CreateTree(tx, "tree1");
				t2 = Env.CreateTree(tx, "tree2");

				tx.Commit();
			}

			await Env.Writer.WriteAsync(batch);

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				using (var stream = t1.Read(tx, "key/1").Stream)
				using (var reader = new StreamReader(stream))
				{
					var result = reader.ReadToEnd();
					Assert.Equal("tree1", result);
				}

				using (var stream = t2.Read(tx, "key/1").Stream)
				using (var reader = new StreamReader(stream))
				{
					var result = reader.ReadToEnd();
					Assert.Equal("tree2", result);
				}
			}
		}

		[Fact]
		public async Task BatchErrorHandling()
		{
			var batch1 = new WriteBatch();
			batch1.Add("key/1", new MemoryStream(Encoding.UTF8.GetBytes("tree1")), "tree1");

			var batch2 = new WriteBatch();
			batch2.Add("key/1", new MemoryStream(Encoding.UTF8.GetBytes("tree2")), "tree2");

			var batch3 = new WriteBatch();
			batch3.Add("key/1", new MemoryStream(Encoding.UTF8.GetBytes("tree3")), "tree3");

			Tree t1;
			Tree t3;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				t1 = Env.CreateTree(tx, "tree1");
				t3 = Env.CreateTree(tx, "tree3");

				tx.Commit();
			}

			try
			{
				await Task.WhenAll(Env.Writer.WriteAsync(batch1), Env.Writer.WriteAsync(batch2), Env.Writer.WriteAsync(batch3));
				Assert.True(false);
			}
			catch (InvalidOperationException e)
			{
				Assert.Equal("No such tree: tree2", e.Message);

				using (var tx = Env.NewTransaction(TransactionFlags.Read))
				{
					using (var stream = t1.Read(tx, "key/1").Stream)
					using (var reader = new StreamReader(stream))
					{
						var result = reader.ReadToEnd();
						Assert.Equal("tree1", result);
					}

					using (var stream = t3.Read(tx, "key/1").Stream)
					using (var reader = new StreamReader(stream))
					{
						var result = reader.ReadToEnd();
						Assert.Equal("tree3", result);
					}
				}
			}
		}

		[Fact]
		public async Task MergedBatchErrorHandling()
		{
			var batch1 = new WriteBatch();
			batch1.Add("key/1", new MemoryStream(Encoding.UTF8.GetBytes("tree1")), "tree1");

			var batch2 = new WriteBatch();
			batch2.Add("key/1", new MemoryStream(Encoding.UTF8.GetBytes("tree2")), "tree2");

			var batch3 = new WriteBatch();
			batch3.Add("key/1", new MemoryStream(Encoding.UTF8.GetBytes("tree3")), "tree3");

			Tree t1;
			Tree t3;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				t1 = Env.CreateTree(tx, "tree1");
				t3 = Env.CreateTree(tx, "tree3");

				tx.Commit();
			}

			Env.Writer._semaphore.Wait(1000); // forcing to build one batch group from all batches that will be added between this line and _semaphore.Release

			var tasks = new[]
				            {
					            Env.Writer.WriteAsync(batch1), 
								Env.Writer.WriteAsync(batch2), 
								Env.Writer.WriteAsync(batch3)
				            };

			Env.Writer._semaphore.Release();

			try
			{
				await Task.WhenAll(tasks);
				Assert.True(false);
			}
			catch (InvalidOperationException e)
			{
				Assert.Equal("No such tree: tree2", e.Message);
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				using (var stream = t1.Read(tx, "key/1").Stream)
				using (var reader = new StreamReader(stream))
				{
					var result = reader.ReadToEnd();
					Assert.Equal("tree1", result);
				}

				using (var stream = t3.Read(tx, "key/1").Stream)
				using (var reader = new StreamReader(stream))
				{
					var result = reader.ReadToEnd();
					Assert.Equal("tree3", result);
				}
			}
		}
	}
}