using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Voron.Tests.Storage
{
	public class BigValues : StorageTest
	{
		[Fact]
		public void CanReuseLargeSpace()
		{
			var random = new Random(43321);
			var buffer = new byte[1024 * 1024 * 6 + 283];
			random.NextBytes(buffer);
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, new Slice(BitConverter.GetBytes(1203)), new MemoryStream(buffer));
				tx.Commit();
			}

			Env.FlushLogToDataFile();

			var old = Env.Options.DataPager.NumberOfAllocatedPages;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Delete(tx, new Slice(BitConverter.GetBytes(1203)));
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				buffer = new byte[1024 * 1024 * 3 + 1238];
				random.NextBytes(buffer);
				tx.State.Root.Add(tx, new Slice(BitConverter.GetBytes(1203)), new MemoryStream(buffer));
				tx.Commit();
			}

			Env.FlushLogToDataFile();

			Assert.Equal(old ,Env.Options.DataPager.NumberOfAllocatedPages);

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var readResult = tx.State.Root.Read(tx, new Slice(BitConverter.GetBytes(1203)));
				Assert.NotNull(readResult);

				var memoryStream = new MemoryStream();
				readResult.Stream.CopyTo(memoryStream);
				Assert.Equal(buffer, memoryStream.ToArray());
				tx.Commit();
			}
		}

		[Fact]
		public void CanStoreInOneTransactionReallyBigValue()
		{
			var random = new Random(43321);
			var buffer = new byte[1024 * 1024 * 15 + 283];
			random.NextBytes(buffer);
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, new Slice(BitConverter.GetBytes(1203)), new MemoryStream(buffer));
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var readResult = tx.State.Root.Read(tx, new Slice(BitConverter.GetBytes(1203)));
				Assert.NotNull(readResult);

				var memoryStream = new MemoryStream();
				readResult.Stream.CopyTo(memoryStream);
				Assert.Equal(buffer, memoryStream.ToArray());
				tx.Commit();
			}
		}

		[Fact]
		public void CanStoreInOneTransactionManySmallValues()
		{
			var buffers = new List<byte[]>();
			var random = new Random(43321);
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 1500; i++)
				{
					var buffer = new byte[912];
					random.NextBytes(buffer);
					buffers.Add(buffer);
					tx.State.Root.Add(tx, new Slice(BitConverter.GetBytes(i)), new MemoryStream(buffer));
				}
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				for (int i = 0; i < 1500; i++)
				{
					var readResult = tx.State.Root.Read(tx, new Slice(BitConverter.GetBytes(i)));
					Assert.NotNull(readResult);

					var memoryStream = new MemoryStream();
					readResult.Stream.CopyTo(memoryStream);
					Assert.Equal(buffers[i], memoryStream.ToArray());

				}
				tx.Commit();
			}
		}
	}
}