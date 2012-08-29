using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SimonCropp : RavenTest
	{
		public class WorkflowItem
		{
			public string Id { get; set; }
			public string WorkflowId { get; set; }
			public string InResponseToId { get; set; }
			public string Text { get; set; }
			public int DisplayOrder { get; set; }
		}

		public class WorkflowSubTasksIndex : AbstractIndexCreationTask<WorkflowItem>
		{
			public WorkflowSubTasksIndex()
			{
				Map = items => from item in items
							   where
								   item.WorkflowId != null && (item.InResponseToId == null || item.WorkflowId != item.InResponseToId)
							   select new
							   {
								   item.DisplayOrder,
								   item.WorkflowId,
								   item.Text,
								   item.Id,
								   item.InResponseToId,
							   };

				Reduce = results => from result in results
									group result by new
									{
										Id = result.InResponseToId ?? result.Id,
									}
										into workflow
										let lastTaskStatus = workflow.OrderByDescending(item => item.DisplayOrder).First()
										select new
										{
											lastTaskStatus.DisplayOrder,
											lastTaskStatus.WorkflowId,
											lastTaskStatus.Text,
											lastTaskStatus.Id,
											lastTaskStatus.InResponseToId,
										};

				TransformResults = (database, items) => from item in items
														let startTask = database.Load<WorkflowItem>(item.InResponseToId ?? item.Id)
														select new
														{
															item.DisplayOrder,
															item.WorkflowId,
															startTask.Text,
															item.Id,
														};

			}
		}

		[Fact]
		public void ThisOneFailson499()
		{
			using (var documentStore = NewDocumentStore())
			{
				new WorkflowSubTasksIndex().Execute(documentStore);
				Setup(documentStore);

				using (var storeSession = documentStore.OpenSession())
				{
					var workflowItems = storeSession.Query<WorkflowItem, WorkflowSubTasksIndex>()
						.Customize(customization => customization.WaitForNonStaleResults())
						.Where(item => item.WorkflowId == "rootDocumentId")
						.ToList();
					Assert.Equal(1, workflowItems.Count);

					Assert.NotNull(workflowItems[0].Text);

					var workflowSubTasks = workflowItems
						.Where(item => item.Text != null)
						.ToList();

					Assert.Equal(1, workflowSubTasks.Count);
				}
			}
		}

		[Fact]
		public void ThisOnePassesOnBoth()
		{
			using (var documentStore = NewDocumentStore())
			{
				new WorkflowSubTasksIndex().Execute(documentStore);
				Setup(documentStore);

				using (var storeSession = documentStore.OpenSession())
				{
					var workflowItems = storeSession.Query<WorkflowItem, WorkflowSubTasksIndex>()
						.Customize(customization => customization.WaitForNonStaleResults())
						.Where(item =>
						       item.WorkflowId == "rootDocumentId" &&
						       item.Text != null
						)
						.ToList();

					Assert.Equal(1, workflowItems.Count);
				}
			}
		}

		public static void Setup(IDocumentStore documentStore)
		{
			using (var storeSession = documentStore.OpenSession())
			{
				var rootDocument = new WorkflowItem
				{
					Id = "rootDocumentId",
					Text = "Workflow 3",
					DisplayOrder = 0,
				};
				storeSession.Store(rootDocument);

				var subTask1Start = new WorkflowItem
				{
					Id = "subTask1StartId",
					WorkflowId = rootDocument.Id,
					Text = "SubTask1",
					DisplayOrder = 0,
				};
				storeSession.Store(subTask1Start);

				storeSession.SaveChanges();
			}
		}
	}
}
