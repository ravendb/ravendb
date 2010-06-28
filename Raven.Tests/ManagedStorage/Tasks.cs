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
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Read(viewer => Assert.False(viewer.Tasks.DoesTasksExistsForIndex("test", null))); 
				tx.Write(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }));
				tx.Read(viewer => Assert.True(viewer.Tasks.DoesTasksExistsForIndex("test", null)));
			}
		}

		[Fact]
		public void CanCheckForExistanceOfTasksWithCutOffs()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				var cutoff = DateTime.UtcNow;
				tx.Write(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }));
				tx.Read(viewer => Assert.True(viewer.Tasks.DoesTasksExistsForIndex("test", null)));
				tx.Read(viewer => Assert.True(viewer.Tasks.DoesTasksExistsForIndex("test", cutoff.AddMinutes(1))));
				tx.Read(viewer => Assert.False(viewer.Tasks.DoesTasksExistsForIndex("test", cutoff.AddMinutes(-1))));
			}
		}

		[Fact]
		public void CanGetTask()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }));
				int tasks;
				tx.Write(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask(out tasks)));
			}
		}

		[Fact]
		public void AfterGettingTaskOnceWillNotGetItAgain()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }));
				int tasks;
				tx.Write(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask(out tasks)));
				tx.Write(mutator => Assert.Null(mutator.Tasks.GetMergedTask(out tasks)));
			}
		}

		[Fact]
		public void CanMergeTasks()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }));
				tx.Write(mutator => mutator.Tasks.AddTask(new MyTask { Index = "test" }));
				int tasks = 0;
				tx.Write(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask(out tasks)));
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
				Id = Id,
				Index = Index,
			};
		}
	}
}