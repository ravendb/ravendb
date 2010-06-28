using Raven.Storage.Managed;
using Xunit;
using System.Linq;

namespace Raven.Storage.Tests
{
	public class Indexes : TxStorageTest
	{
		[Fact]
		public void CanAddAndReadIndex()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Indexing.AddIndex("def"));
				tx.Read(viewer =>
					Assert.True(viewer.Indexing.GetIndexesStats().Any(x => x.Name == "def")));
			}
		}

		[Fact]
		public void CanDeleteIndex()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Indexing.AddIndex("def"));
				tx.Read(viewer =>
					Assert.True(viewer.Indexing.GetIndexesStats().Any(x => x.Name == "def")));

				tx.Write(mutator => mutator.Indexing.DeleteIndex("def"));
				tx.Read(viewer =>
					Assert.False(viewer.Indexing.GetIndexesStats().Any(x => x.Name == "def")));
			}
		}


		[Fact]
		public void CanAddAndReadIndexFailureRate()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Indexing.AddIndex("def"));
				tx.Read(viewer =>
					Assert.Equal("def", viewer.Indexing.GetFailureRate("def").Name));
			}
		}

		[Fact]
		public void CanRecordAttempts()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Indexing.AddIndex("def"));
				tx.Write(mutator=>
				{
					mutator.Indexing.SetCurrentIndexStatsTo("def");

					mutator.Indexing.IncrementIndexingAttempt();

					mutator.Indexing.FlushIndexStats();
				});
				tx.Read(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate("def").Attempts));
			}
		}

		[Fact]
		public void CanRecordAttemptsDecrements()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Indexing.AddIndex("def"));
				tx.Write(mutator =>
				{
					mutator.Indexing.SetCurrentIndexStatsTo("def");

					mutator.Indexing.IncrementIndexingAttempt();

					mutator.Indexing.FlushIndexStats();
				});
				tx.Read(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate("def").Attempts));

				tx.Write(mutator =>
				{
					mutator.Indexing.SetCurrentIndexStatsTo("def");

					mutator.Indexing.DecrementIndexingAttempt();

					mutator.Indexing.FlushIndexStats();
				});
				tx.Read(viewer =>
					Assert.Equal(0, viewer.Indexing.GetFailureRate("def").Attempts));
			}
		}


		[Fact]
		public void CanRecordErrors()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Indexing.AddIndex("def"));
				tx.Write(mutator =>
				{
					mutator.Indexing.SetCurrentIndexStatsTo("def");

					mutator.Indexing.IncrementIndexingFailure();

					mutator.Indexing.FlushIndexStats();
				});
				tx.Read(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate("def").Errors));
			}
		}

		[Fact]
		public void CanRecordSuccesses()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Indexing.AddIndex("def"));
				tx.Write(mutator =>
				{
					mutator.Indexing.SetCurrentIndexStatsTo("def");

					mutator.Indexing.IncrementSuccessIndexing();

					mutator.Indexing.FlushIndexStats();
				});
				tx.Read(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate("def").Successes));
			}
		}
	}
}