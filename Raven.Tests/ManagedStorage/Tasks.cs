//-----------------------------------------------------------------------
// <copyright file="Tasks.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions;
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
				tx.Batch(accessor => accessor.Indexing.AddIndex("test", false));
				tx.Batch(viewer => Assert.False(viewer.Staleness.IsIndexStale("test", null, null))); 
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, SystemTime.Now));
				tx.Batch(viewer => Assert.True(viewer.Staleness.IsIndexStale("test", null, null)));
			}
		}

		[Fact]
		public void CanCheckForExistanceOfTasksAfterTaskWasRemoved()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(accessor => accessor.Indexing.AddIndex("test", false));
				tx.Batch(viewer => Assert.False(viewer.Staleness.IsIndexStale("test", null, null)));
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, SystemTime.Now));
				tx.Batch(viewer => Assert.True(viewer.Staleness.IsIndexStale("test", null, null))); 
				
				int tasks = 0;
				tx.Batch(mutator => mutator.Tasks.GetMergedTask<MyTask>());
				Assert.Equal(1, tasks);
				tx.Batch(viewer => Assert.False(viewer.Staleness.IsIndexStale("test", null, null)));
			}
		}


		[Fact]
		public void CanCheckForExistanceOfTasksWithCutOffs()
		{
			using (var tx = NewTransactionalStorage())
			{
				var cutoff = SystemTime.UtcNow;
				tx.Batch(mutator =>
				{
					mutator.Indexing.AddIndex("test", false);
					mutator.Indexing.UpdateLastIndexed("test", Guid.NewGuid(), cutoff);
				});
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, SystemTime.UtcNow));
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
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, SystemTime.Now));
				tx.Batch(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask<MyTask>()));
			}
		}

		[Fact]
		public void AfterGettingTaskOnceWillNotGetItAgain()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" },SystemTime.Now));
				tx.Batch(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask<MyTask>()));
				tx.Batch(mutator => Assert.Null(mutator.Tasks.GetMergedTask<MyTask>()));
			}
		}

		[Fact]
		public void CanMergeTasks()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, SystemTime.Now));
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, SystemTime.Now));
				int tasks = 0;
				tx.Batch(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask<MyTask>()));
				Assert.Equal(2, tasks);
			}
		}
	}
}