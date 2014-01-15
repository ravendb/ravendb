using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Tests.Storage
{
	using System;
	using System.IO;

	using Xunit;

	public class SplittingVeryBig : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 10 * AbstractPager.PageSize;
		}

		[Fact]
		public void ShouldBeAbleToWriteValuesGreaterThanLogAndReadThem()
		{
			var random = new Random(1234);
			var buffer = new byte[1024 * 512];
			random.NextBytes(buffer);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree");
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.GetTree("tree").Add(tx, "key1", new MemoryStream(buffer));
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
			    var read = tx.GetTree("tree").Read(tx, "key1");
			    Assert.NotNull(read);

			    var reader = read.Reader;
			    Assert.Equal(buffer.Length, read.Reader.Length);
			    Assert.Equal(buffer, reader.ReadBytes(read.Reader.Length));
			}
		}

		[Fact]
		public void ShouldBeAbleToWriteValuesGreaterThanLogAndRecoverThem()
		{
			DeleteDirectory("test2.data");

			var random = new Random(1234);
			var buffer = new byte[1024 * 512];
			random.NextBytes(buffer);

			var options = StorageEnvironmentOptions.ForPath("test2.data");
			options.MaxLogFileSize = 10 * AbstractPager.PageSize;

			using (var env = new StorageEnvironment(options))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "tree");
					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					tx.GetTree("tree").Add(tx, "key1", new MemoryStream(buffer));
					tx.Commit();
				}
			}

			options = StorageEnvironmentOptions.ForPath("test2.data");
			options.MaxLogFileSize = 10 * AbstractPager.PageSize;

			using (var env = new StorageEnvironment(options))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "tree");
					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					var read = tx.GetTree("tree").Read(tx, "key1");
					Assert.NotNull(read);

					{
						Assert.Equal(buffer.Length, read.Reader.Length);
						Assert.Equal(buffer, read.Reader.ReadBytes(read.Reader.Length));
					}
				}
			}

			DeleteDirectory("test2.data");
		}
	}
}