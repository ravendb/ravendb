using System.Diagnostics;
using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class RecoveryMultipleJournals : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 10 * options.DataPager.PageSize;
		}

		[Fact]
		public void CanRecoverAfterRestartWithMultipleFilesInSingleTransaction()
		{

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree");

				tx.Commit();
			}
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < 1000; i++)
				{
					tx.GetTree("tree").Add(tx, "a" + i, new MemoryStream(new byte[100]));
				}
				tx.Commit();
			}


			RestartDatabase();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree");

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				for (var i = 0; i < 1000; i++)
				{
					var readResult = tx.GetTree("tree").Read(tx, "a" + i);
					Assert.NotNull(readResult);
					using (readResult.Stream)
					{
						Assert.Equal(100, readResult.Stream.Length);
					}
				}
				tx.Commit();
			}
		}

		[Fact]
		public void CanResetLogInfoAfterBigUncommitedTransaction()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree");

				tx.Commit();
			}

			var currentJournalInfo = Env.Journal.GetCurrentJournalInfo();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < 1000; i++)
				{
					tx.GetTree("tree").Add(tx, "a" + i, new MemoryStream(new byte[100]));
				}
				//tx.Commit(); - not committing here
			}

			Assert.Equal(currentJournalInfo.CurrentJournal, Env.Journal.GetCurrentJournalInfo().CurrentJournal);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.GetTree("tree").Add(tx, "a", new MemoryStream(new byte[100]));
				tx.Commit();
			}

			Assert.Equal(currentJournalInfo.CurrentJournal, Env.Journal.GetCurrentJournalInfo().CurrentJournal);
		}

		[Fact]
		public void CanResetLogInfoAfterBigUncommitedTransaction2()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree");

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < 1000; i++)
				{
					tx.GetTree("tree").Add(tx, "a" + i, new MemoryStream(new byte[100]));
				}
				tx.Commit(); 
			}

			var currentJournalInfo = Env.Journal.GetCurrentJournalInfo();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < 1000; i++)
				{
					tx.GetTree("tree").Add(tx, "b" + i, new MemoryStream(new byte[100]));
				}
				//tx.Commit(); - not committing here
			}

			Assert.Equal(currentJournalInfo.CurrentJournal, Env.Journal.GetCurrentJournalInfo().CurrentJournal);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.GetTree("tree").Add(tx, "b", new MemoryStream(new byte[100]));
				tx.Commit();
			}

			Assert.Equal(currentJournalInfo.CurrentJournal +1, Env.Journal.GetCurrentJournalInfo().CurrentJournal);
		}

		[Fact]
		public void CanResetLogInfoAfterBigUncommitedTransactionWithRestart()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree");

				tx.Commit();
			}

			var currentJournalInfo = Env.Journal.GetCurrentJournalInfo();


			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < 1000; i++)
				{
					tx.GetTree("tree").Add(tx, "a" + i, new MemoryStream(new byte[100]));
				}
				tx.Commit();
			}

			var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

			StopDatabase();

			CorruptPage(lastJournal - 1, page: 2, pos: 3);

			StartDatabase();
			Assert.Equal(currentJournalInfo.CurrentJournal, Env.Journal.GetCurrentJournalInfo().CurrentJournal);

		}

		[Fact]
		public void CanResetLogInfoAfterBigUncommitedTransactionWithRestart2()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree");

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < 1000; i++)
				{
					tx.GetTree("tree").Add(tx, "a" + i, new MemoryStream(new byte[100]));
				}
				tx.Commit();
			}

			var currentJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < 1000; i++)
				{
					tx.GetTree("tree").Add(tx, "b" + i, new MemoryStream(new byte[100]));
				}
				tx.Commit();
			}

			var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

			StopDatabase();

			CorruptPage(lastJournal - 1, page: 2, pos: 3);

			StartDatabase();
			Assert.Equal(currentJournal, Env.Journal.GetCurrentJournalInfo().CurrentJournal);

		}


		[Fact]
		public void CorruptingOneTransactionWillKillAllFutureTransactions()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree");

				tx.Commit();
			}

			for (int i = 0; i < 1000; i++)
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					tx.GetTree("tree").Add(tx, "a" + i, new MemoryStream(new byte[100]));
					tx.Commit();
				}
			}

			var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;
			var lastJournalPosition = Env.Journal.CurrentFile.WritePagePosition;

			StopDatabase();

			CorruptPage(lastJournal - 3, lastJournalPosition + 1, 5);

			StartDatabase();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree");

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Assert.Null(tx.GetTree("tree").Read(tx, "a999"));
			}

		}

		private unsafe void CorruptPage(long journal, long page, int pos)
		{
			var journalPager = _options.CreateJournalPager(journal);
			var writable = journalPager.GetWritable(page);
			*(writable.Base + pos) = 42;
		}
	}
}