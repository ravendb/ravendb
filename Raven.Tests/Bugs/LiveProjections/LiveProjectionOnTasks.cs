using Raven.Abstractions;
using Raven.Client;

namespace Raven.Tests.Bugs.LiveProjections
{
	using System;
	using System.Linq;
	using Raven.Client.Linq;
	using Raven.Tests.Bugs.LiveProjections.Entities;
	using Raven.Tests.Bugs.LiveProjections.Indexes;
	using Raven.Tests.Bugs.LiveProjections.Views;
	using Xunit;

	public class LiveProjectionOnTasks : RavenTest
	{
		[Fact]
		public void TaskLiveProjection()
		{
			using (var documentStore = NewDocumentStore())
			{
				new TaskSummaryIndex().Execute(((IDocumentStore)documentStore).DatabaseCommands, ((IDocumentStore)documentStore).Conventions);

				using (var session = documentStore.OpenSession())
				{
					session.Store(
						new User() { Name = "John Doe" }
					);

					session.Store(
						new User() { Name = "Bob Smith" }
					);

					session.Store(
						new Place() { Name = "Coffee Shop" }
					);

					session.Store(
						new Task()
						{
							Description = "Testing projections",
							Start = SystemTime.UtcNow,
							End = SystemTime.UtcNow.AddMinutes(30),
							GiverId = 1,
							TakerId = 2,
							PlaceId = 1
						}
					);

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<dynamic, TaskSummaryIndex>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.As<TaskSummary>()
						.ToList();

					var first = results.FirstOrDefault();

					Assert.NotNull(first);
					Assert.Equal(first.Id, "tasks/1");
					Assert.Equal(first.GiverId, 1);
				}
			}
		}
	}
}
