// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1435.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
	using System;
	using System.Linq;

	using Raven.Abstractions.Data;
	using Raven.Database.Tasks;

	using Xunit;
	using Xunit.Extensions;

	public class RavenDB_1435 : RavenTest
	{
		[Theory]
        [PropertyData("Storages")]
		public void ShouldWork(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.Equal(0, accessor.Tasks.GetPendingTasksForDebug().Count()));

				var date1 = DateTime.Now;
				var date2 = date1.AddMinutes(1);

				storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask()
																 {
																	 Index = 1
																 }, date1));

				storage.Batch(accessor =>
				{
					var tasks = accessor.Tasks
						.GetPendingTasksForDebug()
						.ToList();

					Assert.Equal(1, tasks.Count);

					var task = tasks[0];

					AssertTask(task, task.Id is Etag ? (object)Etag.Parse("08000000-0000-0000-0000-000000000001") : 1, date1,  1, typeof(RemoveFromIndexTask));
				});

				storage.Batch(accessor => accessor.Tasks.AddTask(new TouchMissingReferenceDocumentTask
																 {
																	 Index = 2
																 }, date2));

				storage.Batch(accessor =>
				{
					var tasks = accessor.Tasks
						.GetPendingTasksForDebug()
						.ToList();

					Assert.Equal(2, tasks.Count);

					TaskMetadata t1 = null;
					TaskMetadata t2 = null;
					if (tasks[0].IndexId == 1)
					{
						t1 = tasks[0];
						t2 = tasks[1];
					}
					else
					{
						t1 = tasks[1];
						t2 = tasks[0];
					}

                    AssertTask(t1, t1.Id is Etag ? (object)Etag.Parse("08000000-0000-0000-0000-000000000001") : 1, date1, 1, typeof(RemoveFromIndexTask));
                    AssertTask(t2, t2.Id is Etag ? (object)Etag.Parse("08000000-0000-0000-0000-000000000002") : 2, date2, 2, typeof(TouchMissingReferenceDocumentTask));
				});
			}
		}

		private void AssertTask(TaskMetadata task, object expectedId, DateTime expectedAddedTime, int expecedIndex, Type expectedType)
		{
			Assert.Equal(expectedType.FullName, task.Type);
			Assert.Equal(expectedAddedTime, task.AddedTime);
			Assert.Equal(expecedIndex, task.IndexId);
			Assert.Equal(expectedId, task.Id);
		}
	}
}