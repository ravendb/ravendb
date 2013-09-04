// -----------------------------------------------------------------------
//  <copyright file="TasksStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Storage.Voron
{
	using System;

	using Raven.Database.Tasks;

	using Xunit;

	public class TasksStorageActions : RavenTest
	{
		[Fact]
		public void SimpleTask()
		{
			using (var storage = NewTransactionalStorage(requestedStorage: "voron"))
			{
				storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask(), DateTime.Now));

				storage.Batch(accessor =>
				{
					Assert.True(accessor.Tasks.HasTasks);
					Assert.Equal(1, accessor.Tasks.ApproximateTaskCount);
				});

				storage.Batch(accessor =>
				{
					var task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>();
					Assert.NotNull(task);
				});

				storage.Batch(accessor =>
				{
					Assert.False(accessor.Tasks.HasTasks);
					Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);
				});
			}
		}

		[Fact]
		public void MergingTask()
		{
			using (var storage = NewTransactionalStorage(requestedStorage: "voron"))
			{
				storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask(), DateTime.Now));
				storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask(), DateTime.Now));

				storage.Batch(accessor =>
				{
					Assert.True(accessor.Tasks.HasTasks);
					Assert.Equal(2, accessor.Tasks.ApproximateTaskCount);
				});

				storage.Batch(accessor =>
				{
					var task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>();
					Assert.NotNull(task);
				});

				storage.Batch(accessor =>
				{
					Assert.False(accessor.Tasks.HasTasks);
					Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);
				});
			}
		}
	}
}