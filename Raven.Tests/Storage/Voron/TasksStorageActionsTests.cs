// -----------------------------------------------------------------------
//  <copyright file="TasksStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

namespace Raven.Tests.Storage.Voron
{
	using System;

	using Raven.Database.Tasks;

	using Xunit;
	using Xunit.Extensions;

	[Trait("VoronTest", "StorageActionsTests")]
	public class TasksStorageActionsTests : TransactionalStorageTestBase
	{
		[Theory]
		[PropertyData("Storages")]
		public void SimpleTask(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask { Index = 101 }, DateTime.Now));

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

		[Theory]
		[PropertyData("Storages")]
		public void MergingTask(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask { Index = 101 }, DateTime.Now));
				storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask { Index = 101 }, DateTime.Now));

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