// -----------------------------------------------------------------------
//  <copyright file="FlushingToDataFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class FlushingToDataFile : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.ManualFlushing = true;
			options.MaxLogFileSize = 2 * AbstractPager.PageSize;
		}

		[Fact]
		public unsafe void ReadTransactionShouldNotReadFromJournalSnapshotIfJournalWasFlushedInTheMeanwhile()
		{
			var value1 = new byte[4000];

			new Random().NextBytes(value1);

			Assert.Equal(2 * AbstractPager.PageSize, Env.Options.MaxLogFileSize);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "foo/0", new MemoryStream(value1));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "foo/1", new MemoryStream(value1));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Env.FlushLogToDataFile(); // force flushing during read transaction

				using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					// empty transaction is enough to expose the issue because it allocates 1 page in the scratch space for the transaction header
					txw.Commit();
				}

				for (var i = 0; i < 2; i++)
				{
					var readResult = tx.State.Root.Read(tx, "foo/" + i);

					Assert.NotNull(readResult);
					Assert.Equal(value1.Length, readResult.Reader.Length);

                    var memoryStream = new MemoryStream(readResult.Reader.Length);
					readResult.Reader.CopyTo(memoryStream);

					fixed (byte* b = value1)
					fixed (byte* c = memoryStream.GetBuffer())
						Assert.Equal(0, NativeMethods.memcmp(b, c, value1.Length));
				}
			}
		}

		[Fact]
		public void FlushingOperationShouldHaveOwnScratchPagerStateReference()
		{
			var value1 = new byte[4000];

			new Random().NextBytes(value1);

			Assert.Equal(2 * AbstractPager.PageSize, Env.Options.MaxLogFileSize);

			Env.FlushLogToDataFile();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "foo/0", new MemoryStream(value1));
				tx.State.Root.Add(tx, "foo/1", new MemoryStream(value1));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "foo/0", new MemoryStream(value1));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "foo/4", new MemoryStream(value1));
				tx.Commit();
			}


			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var readResult = tx.State.Root.Read(tx, "foo/0");

				Assert.NotNull(readResult);
				Assert.Equal(value1.Length, readResult.Reader.Length);

				var memoryStream = new MemoryStream();
				readResult.Reader.CopyTo(memoryStream);
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Env.FlushLogToDataFile();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var readResult = tx.State.Root.Read(tx, "foo/0");

				Assert.NotNull(readResult);
				Assert.Equal(value1.Length, readResult.Reader.Length);

				var memoryStream = new MemoryStream();
				readResult.Reader.CopyTo(memoryStream);
			}
		}
	}
}