//-----------------------------------------------------------------------
// <copyright file="Indexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class Indexes : RavenTest
	{
		[Fact]
		public void CanAddAndReadIndex()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex("def", false));
				tx.Batch( viewer =>
					Assert.True(viewer.Indexing.GetIndexesStats().Any(x => x.Name == "def")));
			}
		}

		[Fact]
		public void CanDeleteIndex()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex("def", false));
				tx.Batch(viewer =>
					Assert.True(viewer.Indexing.GetIndexesStats().Any(x => x.Name == "def")));

				tx.Batch(mutator => mutator.Indexing.DeleteIndex("def"));
				tx.Batch(viewer =>
					Assert.False(viewer.Indexing.GetIndexesStats().Any(x => x.Name == "def")));
			}
		}


		[Fact]
		public void CanAddAndReadIndexFailureRate()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex("def", false));
				tx.Batch(viewer =>
					Assert.Equal("def", viewer.Indexing.GetFailureRate("def").Name));
			}
		}

		[Fact]
		public void CanRecordAttempts()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex("def", false));
				tx.Batch(mutator=>
				{
					mutator.Indexing.UpdateIndexingStats("def", new IndexingWorkStats
					{
						IndexingAttempts = 1
					});

				});
				tx.Batch(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate("def").Attempts));
			}
		}

		[Fact]
		public void CanRecordAttemptsDecrements()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex("def", false));
				tx.Batch(mutator =>
				{
					mutator.Indexing.UpdateIndexingStats("def", new IndexingWorkStats
					{
						IndexingAttempts = 1
					});
				});
				tx.Batch(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate("def").Attempts));

				tx.Batch(mutator =>
				{
					mutator.Indexing.UpdateIndexingStats("def", new IndexingWorkStats
					{
						IndexingAttempts = -1
					});
				});

				tx.Batch(viewer =>
					Assert.Equal(0, viewer.Indexing.GetFailureRate("def").Attempts));
			}
		}


		[Fact]
		public void CanRecordErrors()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex("def", false));
				tx.Batch(mutator =>
				{
					mutator.Indexing.UpdateIndexingStats("def", new IndexingWorkStats
					{
						IndexingErrors = 1
					});

				});
				tx.Batch(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate("def").Errors));
			}
		}

		[Fact]
		public void CanRecordSuccesses()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex("def", false));
				tx.Batch(mutator =>
				{
					mutator.Indexing.UpdateIndexingStats("def", new IndexingWorkStats
					{
						IndexingSuccesses = 1
					});
				});
				tx.Batch(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate("def").Successes));
			}
		}
	}
}