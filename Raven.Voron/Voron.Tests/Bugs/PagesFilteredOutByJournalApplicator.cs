using System.IO;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class PagesFilteredOutByJournalApplicator : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			base.Configure(options);
			options.ManualFlushing = true;
		}

		[Fact]
		public void CouldNotReadPagesThatWereFilteredOutByJournalApplicator_1()
		{
			var bytes = new byte[1000];

			using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(txw, "foo");

				tree.Add(txw, "bars/1", new MemoryStream(bytes));

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
				Env.CreateTree(txw, "baz");

				txw.Commit();
			}

			using (var txr = Env.NewTransaction(TransactionFlags.Read))
			{
				using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = Env.State.GetTree(txw, "foo");

					tree.Add(txw, "bars/1", new MemoryStream());

					txw.Commit();

					RenderAndShow(txw, tree, 1);
				}

				Env.FlushLogToDataFile();

				Assert.NotNull(Env.State.GetTree(txr, "foo").Read(txr, "bars/1"));
			}
		} 

		[Fact]
		public void CouldNotReadPagesThatWereFilteredOutByJournalApplicator_2()
		{
			var bytes = new byte[1000];

			using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(txw, "foo");

				tree.Add(txw, "bars/1", new MemoryStream(bytes));
				tree.Add(txw, "bars/2", new MemoryStream(bytes));
				tree.Add(txw, "bars/3", new MemoryStream(bytes));
				tree.Add(txw, "bars/4", new MemoryStream(bytes));

				txw.Commit();

				RenderAndShow(txw, tree, 1);
			}

			using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.State.GetTree(txw, "foo");

				tree.Add(txw, "bars/0", new MemoryStream());
				tree.Add(txw, "bars/5", new MemoryStream());

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
				Env.CreateTree(txw, "baz");

				txw.Commit();
			}

			using (var txr = Env.NewTransaction(TransactionFlags.Read))
			{
				using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = Env.State.GetTree(txw, "foo");

					tree.Add(txw, "bars/4", new MemoryStream());

					txw.Commit();

					RenderAndShow(txw, tree, 1);
				}

				Env.FlushLogToDataFile();

				Assert.NotNull(Env.State.GetTree(txr, "foo").Read(txr, "bars/5"));
			}
		}
	}
}
