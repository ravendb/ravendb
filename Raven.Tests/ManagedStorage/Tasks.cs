//-----------------------------------------------------------------------
// <copyright file="Tasks.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Xunit;

namespace Raven.Tests.ManagedStorage
{
	public class Tasks : TxStorageTest
	{
		[Fact]
		public void CanCheckForExistanceOfTasks()
		{
			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(accessor => accessor.Indexing.AddIndex("test"));
                tx.Batch(viewer => Assert.False(viewer.Staleness.IsIndexStale("test", null, null))); 
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, DateTime.Now));
                tx.Batch(viewer => Assert.True(viewer.Staleness.IsIndexStale("test", null, null)));
			}
		}

		[Fact]
		public void CanCheckForExistanceOfTasksAfterTaskWasRemoved()
		{
			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(accessor => accessor.Indexing.AddIndex("test"));
                tx.Batch(viewer => Assert.False(viewer.Staleness.IsIndexStale("test", null, null)));
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, DateTime.Now));
                tx.Batch(viewer => Assert.True(viewer.Staleness.IsIndexStale("test", null, null))); 
				
				int tasks = 0;
				tx.Batch(mutator => mutator.Tasks.GetMergedTask(out tasks));
				Assert.Equal(1, tasks);
                tx.Batch(viewer => Assert.False(viewer.Staleness.IsIndexStale("test", null, null)));
			}
		}


		[Fact]
		public void CanCheckForExistanceOfTasksWithCutOffs()
		{
			using (var tx = NewTransactionalStorage())
			{
				var cutoff = DateTime.UtcNow;
                tx.Batch(mutator =>
                {
                    mutator.Indexing.AddIndex("test");
                    mutator.Indexing.UpdateLastIndexed("test", Guid.NewGuid(), cutoff);
                });
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, DateTime.UtcNow));
                tx.Batch(viewer => Assert.True(viewer.Staleness.IsIndexStale("test", null, null)));
                tx.Batch(viewer => Assert.True(viewer.Staleness.IsIndexStale("test", cutoff.AddMinutes(1), null)));
                tx.Batch(viewer => Assert.False(viewer.Staleness.IsIndexStale("test", cutoff.AddMinutes(-1), null)));
			}
		}

		[Fact]
		public void CanGetTask()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, DateTime.Now));
				int tasks;
				tx.Batch(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask(out tasks)));
			}
		}

		[Fact]
		public void AfterGettingTaskOnceWillNotGetItAgain()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" },DateTime.Now));
				int tasks;
				tx.Batch(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask(out tasks)));
				tx.Batch(mutator => Assert.Null(mutator.Tasks.GetMergedTask(out tasks)));
			}
		}

		[Fact]
		public void CanMergeTasks()
		{
			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, DateTime.Now));
                tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, DateTime.Now));
				int tasks = 0;
				tx.Batch(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask(out tasks)));
				Assert.Equal(2, tasks);
			}
		}
	}
}