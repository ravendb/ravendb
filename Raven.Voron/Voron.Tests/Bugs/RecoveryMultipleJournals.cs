using System;
using System.Diagnostics;
using System.IO;
using System.Linq.Expressions;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class RecoveryMultipleJournals : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 10 * AbstractPager.PageSize;
			options.OnRecoveryError += (sender, args) => { }; // just shut it up
			options.ManualFlushing = true;
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
					{
						Assert.Equal(100, readResult.Reader.Length);
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

			var random = new Random();
			var buffer = new byte[1000000];
			random.NextBytes(buffer);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < 1000; i++)
				{
					tx.GetTree("tree").Add(tx, "b" + i, new MemoryStream(buffer));
				}
				//tx.Commit(); - not committing here
			}

			Assert.Equal(currentJournalInfo.CurrentJournal, Env.Journal.GetCurrentJournalInfo().CurrentJournal);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.GetTree("tree").Add(tx, "b", new MemoryStream(buffer));
				tx.Commit();
			}

			Assert.Equal(currentJournalInfo.CurrentJournal +1, Env.Journal.GetCurrentJournalInfo().CurrentJournal);
		}

		[Fact]
		public void CanResetLogInfoAfterBigUncommitedTransactionWithRestart()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree").Add(tx, "exists", new MemoryStream(new byte[100]));

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

			var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;
            
			StopDatabase();
			
            CorruptPage(lastJournal, page: 4, pos: 3);

			StartDatabase();
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "tree");
                Assert.NotNull(tree.Read(tx, "exists"));
                Assert.Null(tree.Read(tx, "a1"));
                Assert.Null(tree.Read(tx, "a100"));
                Assert.Null(tree.Read(tx, "a500"));
                Assert.Null(tree.Read(tx, "a1000"));
                
                tx.Commit();
            }
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

		private void CorruptPage(long journal, long page, int pos)
		{
			_options.Dispose();
			_options = StorageEnvironmentOptions.ForPath("test.data");
			Configure(_options);
			using (var fileStream = new FileStream(
				Path.Combine("test.data", StorageEnvironmentOptions.JournalName(journal)), 
				FileMode.Open,
				FileAccess.ReadWrite, 
				FileShare.ReadWrite | FileShare.Delete))
		    {
		        fileStream.Position = page*AbstractPager.PageSize;

				var buffer = new byte[AbstractPager.PageSize];

		        var remaining = buffer.Length;
		        var start = 0;
		        while (remaining > 0)
		        {
		            var read = fileStream.Read(buffer, start, remaining);
		            if (read == 0)
		                break;
		            start += read;
		            remaining -= read;
		        }

		        buffer[pos] = 42;
				fileStream.Position = page * AbstractPager.PageSize;
		        fileStream.Write(buffer, 0, buffer.Length);
		    }
		}
	}
}
