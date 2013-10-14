//-----------------------------------------------------------------------
// <copyright file="Indexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading;
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
				tx.Batch(mutator => mutator.Indexing.AddIndex(555, false));
				tx.Batch( viewer =>
					Assert.True(viewer.Indexing.GetIndexesStats().Any(x => x.Id == 555)));
			}
		}

		[Fact]
		public void CanDeleteIndex()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex(555, false));
				tx.Batch(viewer =>
					Assert.True(viewer.Indexing.GetIndexesStats().Any(x => x.Id == 555)));

				tx.Batch(mutator =>
				{
					mutator.Indexing.PrepareIndexForDeletion(555);
					mutator.Indexing.DeleteIndex(555, new CancellationToken());
				});
				tx.Batch(viewer =>
					Assert.False(viewer.Indexing.GetIndexesStats().Any(x => x.Id == 555)));
			}
		}


		[Fact]
		public void CanAddAndReadIndexFailureRate()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex(555, false));
				tx.Batch(viewer =>
					Assert.Equal(555, viewer.Indexing.GetFailureRate(555).Id));
			}
		}

		[Fact]
		public void CanRecordAttempts()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex(555, false));
				tx.Batch(mutator=> mutator.Indexing.UpdateIndexingStats(555, new IndexingWorkStats
				{
					IndexingAttempts = 1
				}));
				tx.Batch(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate(555).Attempts));
			}
		}

		[Fact]
		public void CanRecordAttemptsDecrements()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex(555, false));
				tx.Batch(mutator => mutator.Indexing.UpdateIndexingStats(555, new IndexingWorkStats
				{
					IndexingAttempts = 1
				}));
				tx.Batch(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate(555).Attempts));

				tx.Batch(mutator => mutator.Indexing.UpdateIndexingStats(555, new IndexingWorkStats
				{
					IndexingAttempts = -1
				}));

				tx.Batch(viewer =>
					Assert.Equal(0, viewer.Indexing.GetFailureRate(555).Attempts));
			}
		}


		[Fact]
		public void CanRecordErrors()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex(555, false));
				tx.Batch(mutator => mutator.Indexing.UpdateIndexingStats(555, new IndexingWorkStats
				{
					IndexingErrors = 1
				}));
				tx.Batch(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate(555).Errors));
			}
		}

		[Fact]
		public void CanRecordSuccesses()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Indexing.AddIndex(555, false));
				tx.Batch(mutator => mutator.Indexing.UpdateIndexingStats(555, new IndexingWorkStats
				{
					IndexingSuccesses = 1
				}));
				tx.Batch(viewer =>
					Assert.Equal(1, viewer.Indexing.GetFailureRate(555).Successes));
			}
		}
	}
}
