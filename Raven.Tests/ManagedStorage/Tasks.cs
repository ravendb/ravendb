using System;
using Raven.Database.Indexing;
using Raven.Database.Tasks;
using Raven.Storage.Managed;
using Xunit;

namespace Raven.Storage.Tests
{
	public class Tasks : TxStorageTest
	{
		[Fact]
		public void CanCheckForExistanceOfTasks()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(viewer => Assert.False(viewer.Tasks.IsIndexStale("test", null, null))); 
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, DateTime.Now));
				tx.Batch(viewer => Assert.True(viewer.Tasks.IsIndexStale("test", null, null)));
			}
		}

		[Fact]
		public void CanCheckForExistanceOfTasksAfterTaskWasRemoved()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(viewer => Assert.False(viewer.Tasks.IsIndexStale("test", null, null)));
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, DateTime.Now));
				tx.Batch(viewer => Assert.True(viewer.Tasks.IsIndexStale("test", null, null))); 
				
				int tasks = 0;
				tx.Batch(mutator => mutator.Tasks.GetMergedTask(out tasks));
				Assert.Equal(1, tasks);
                tx.Batch(viewer => Assert.False(viewer.Tasks.IsIndexStale("test", null, null)));
			}
		}


		[Fact]
		public void CanCheckForExistanceOfTasksWithCutOffs()
		{
			using (var tx = NewTransactionalStorage())
			{
				var cutoff = DateTime.UtcNow;
				tx.Batch(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }, DateTime.Now));
                tx.Batch(viewer => Assert.True(viewer.Tasks.IsIndexStale("test", null, null)));
                tx.Batch(viewer => Assert.True(viewer.Tasks.IsIndexStale("test", cutoff.AddMinutes(1), null)));
                tx.Batch(viewer => Assert.False(viewer.Tasks.IsIndexStale("test", cutoff.AddMinutes(-1), null)));
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

	public class MyTask : Task
	{
		public override bool TryMerge(Task task)
		{
			return true;
		}

		public override void Execute(WorkContext context)
		{
			
		}

		public override Task Clone()
		{
			return new MyTask
			{
				Index = Index,
			};
		}
	}
}