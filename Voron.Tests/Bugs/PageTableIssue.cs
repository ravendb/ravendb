using System;
using System.IO;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class PageTableIssue : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			base.Configure(options);
			options.ManualFlushing = true;
		}

		[Fact]
		public void MissingScratchPagesInPageTable()
		{
			var bytes = new byte[1000];

			using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree1 = Env.CreateTree(txw, "foo");
				var tree2 = Env.CreateTree(txw, "bar");
				var tree3 = Env.CreateTree(txw, "baz");

				tree1.Add(txw, "foos/1", new MemoryStream(bytes));

				txw.Commit();

				RenderAndShow(txw, tree1, 1);
			}

			using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.State.GetTree(txw, "bar");

				tree.Add(txw, "bars/1", new MemoryStream(bytes));

				txw.Commit();

				RenderAndShow(txw, tree, 1);
			}

			var bytesToFillFirstJournalCompletely = new byte[8*AbstractPager.PageSize];

			new Random().NextBytes(bytesToFillFirstJournalCompletely);

			using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.State.GetTree(txw, "baz");

				// here we have to put a big value to be sure that in next transaction we will put the
				// updated value into a new journal file - this is the key to expose the issue
				tree.Add(txw, "bazs/1", new MemoryStream(bytesToFillFirstJournalCompletely));

				txw.Commit();

				RenderAndShow(txw, tree, 1);
			}

			using (var txr = Env.NewTransaction(TransactionFlags.Read))
			{
				using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = Env.State.GetTree(txw, "foo");

					tree.Add(txw, "foos/1", new MemoryStream());

					txw.Commit();

					RenderAndShow(txw, tree, 1);
				}

				Env.FlushLogToDataFile();

				Assert.NotNull(Env.State.GetTree(txr, "foo").Read(txr, "foos/1"));
			}
		}
	}
}
