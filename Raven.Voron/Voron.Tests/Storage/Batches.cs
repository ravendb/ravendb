namespace Voron.Tests.Storage
{
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
				using (var stream = Env.Root.Read(tx, "key/1"))
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
					using (var stream = Env.Root.Read(tx, "key/" + i))
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
					using (var stream = Env.Root.Read(tx, "key/" + i))
					using (var reader = new StreamReader(stream))
					{
						var result = reader.ReadToEnd();
						Assert.Equal(i.ToString(CultureInfo.InvariantCulture), result);
					}

					using (var stream = Env.Root.Read(tx, "yek/" + i))
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
			}

			await Task.WhenAll(Env.Writer.WriteAsync(batch1), Env.Writer.WriteAsync(batch2));

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				for (int i = 0; i < numberOfItems; i++)
				{
					using (var stream = t1.Read(tx, "key/" + i))
					using (var reader = new StreamReader(stream))
					{
						var result = reader.ReadToEnd();
						Assert.Equal(i.ToString(CultureInfo.InvariantCulture), result);
					}

					using (var stream = t2.Read(tx, "yek/" + i))
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
			}

			await Env.Writer.WriteAsync(batch);

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				using (var stream = t1.Read(tx, "key/1"))
				using (var reader = new StreamReader(stream))
				{
					var result = reader.ReadToEnd();
					Assert.Equal("tree1", result);
				}

				using (var stream = t2.Read(tx, "key/1"))
				using (var reader = new StreamReader(stream))
				{
					var result = reader.ReadToEnd();
					Assert.Equal("tree2", result);
				}
			}
		}
	}
}