// -----------------------------------------------------------------------
//  <copyright file="ForScratchBuffer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Voron.Util;
using Xunit;

namespace Voron.Tests.ScratchBuffer
{
	public class ScratchCanForceToFlushOldPages: StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			base.Configure(options);
			options.ManualFlushing = true;
		}

		[Fact]
		public void CanForceToFlushPagesOlderThanOldestActiveTransactionToFreePagesFromScratch()
		{
			using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(txw, "foo");

				tree.Add("bars/1", new string('a', 1000));

				txw.Commit();

				RenderAndShow(txw, tree, 1);
			}

			using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(txw, "bar");

				txw.Commit();
			}

			using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.State.GetTree(txw, "foo");

				tree.Add("bars/1", new string('b', 1000));

				txw.Commit();

				RenderAndShow(txw, tree, 1);
			}

			var txr = Env.NewTransaction(TransactionFlags.Read);
			{
				using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = Env.State.GetTree(txw, "foo");

					tree.Add("bars/1", new string('c', 1000));

					txw.Commit();

					RenderAndShow(txw, tree, 1);
				}

				Env.FlushLogToDataFile();

				txr.Dispose();

				using (var txr2 = Env.NewTransaction(TransactionFlags.Read))
				{
					var allocated1 = Env.ScratchBufferPool.GetNumberOfAllocations(0);

					Env.FlushLogToDataFile();

					var allocated2 = Env.ScratchBufferPool.GetNumberOfAllocations(0);

					Assert.Equal(allocated1, allocated2);

					Env.FlushLogToDataFile(allowToFlushOverwrittenPages: true);

					var allocated3 = Env.ScratchBufferPool.GetNumberOfAllocations(0);

					Assert.True(allocated3 < allocated2);

					var read = Env.State.GetTree(txr2, "foo").Read("bars/1");

					Assert.NotNull(read);
					Assert.Equal(new string('c', 1000), read.Reader.AsSlice().ToString());
				}
			}
		} 
	}
}