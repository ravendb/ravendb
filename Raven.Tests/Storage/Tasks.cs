//-----------------------------------------------------------------------
// <copyright file="Tasks.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Tasks;
using Xunit;

namespace Raven.Tests.Storage
{
	public class Tasks : RavenTest
	{
		[Fact]
		public void CanCheckForExistenceOfTasks()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.BatchRead(accessor => accessor.Indexing.AddIndex("test", false));
				tx.BatchRead(viewer => Assert.False(viewer.Staleness.IsIndexStale("test", null, null)));
				tx.BatchRead(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask { Index = "test" }, SystemTime.UtcNow));
				tx.BatchRead(viewer => Assert.True(viewer.Staleness.IsIndexStale("test", null, null)));
			}
		}

		[Fact]
		public void CanCheckForExistenceOfTasksAfterTaskWasRemoved()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.BatchRead(accessor => accessor.Indexing.AddIndex("test", false));
				tx.BatchRead(viewer => Assert.False(viewer.Staleness.IsIndexStale("test", null, null)));
				tx.BatchRead(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask { Index = "test" }, SystemTime.UtcNow));
				tx.BatchRead(viewer => Assert.True(viewer.Staleness.IsIndexStale("test", null, null)));

				tx.BatchRead(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask<RemoveFromIndexTask>()));
				tx.BatchRead(viewer => Assert.False(viewer.Staleness.IsIndexStale("test", null, null)));
			}
		}


		[Fact]
		public void CanCheckForExistenceOfTasksWithCutOffs()
		{
			using (var tx = NewTransactionalStorage())
			{
				var cutoff = SystemTime.UtcNow;
				tx.BatchRead(mutator =>
				{
					mutator.Indexing.AddIndex("test", false);
					mutator.Indexing.UpdateLastIndexed("test", Etag.InvalidEtag, cutoff);
				});
				tx.BatchRead(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask { Index = "test" }, SystemTime.UtcNow));
				tx.BatchRead(viewer => Assert.True(viewer.Staleness.IsIndexStale("test", null, null)));
				tx.BatchRead(viewer => Assert.True(viewer.Staleness.IsIndexStale("test", cutoff.AddMinutes(1), null)));
				tx.BatchRead(viewer => Assert.False(viewer.Staleness.IsIndexStale("test", cutoff.AddMinutes(-1), null)));
			}
		}

		[Fact]
		public void CanGetTask()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.BatchRead(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask { Index = "test" }, SystemTime.UtcNow));
				tx.BatchRead(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask<RemoveFromIndexTask>()));
			}
		}

		[Fact]
		public void AfterGettingTaskOnceWillNotGetItAgain()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.BatchRead(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask { Index = "test" }, SystemTime.UtcNow));
				tx.BatchRead(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask<RemoveFromIndexTask>()));
				tx.BatchRead(mutator => Assert.Null(mutator.Tasks.GetMergedTask<RemoveFromIndexTask>()));
			}
		}

		[Fact]
		public void CanMergeTasks()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.BatchRead(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask { Index = "test", Keys = {"a"}}, SystemTime.UtcNow));
				tx.BatchRead(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask { Index = "test", Keys = {"b"}}, SystemTime.UtcNow));
				tx.BatchRead(mutator => Assert.Equal(2, mutator.Tasks.GetMergedTask<RemoveFromIndexTask>().Keys.Count));
			}
		}
	}
}
