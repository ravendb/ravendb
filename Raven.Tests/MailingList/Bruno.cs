using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Bruno : RavenTest
	{
		[Fact]
		public void StrangeReduceOnNestedItems()
		{
			using (var store = NewDocumentStore())
			{
				new TasksCount_ForPerson().Execute(store);

				var userA = new Person
				{
					Id = "person/1",
					Name = "User A"
				};
				var userB = new Person
				{
					Id = "person/2",
					Name = "User B"
				};
				using (var session = store.OpenSession())
				{
					session.Store(userA);
					session.Store(userB);
					session.Store(new Project
					{
						Name = "Proj A",
						Activities = new List<Activity>
						{
							new Activity
							{
								Name = "Activity A",
								Tasks = new List<Task>
								{
									new Task
									{
										Name = "Task A",
										Owner = userA
									},
									new Task
									{
										Name = "Task B",
										Owner = userA
									},
								}
							},
							new Activity
							{
								Name = "Activity B",
								Tasks = new List<Task>
								{
									new Task
									{
										Name = "Task C",
										Owner = userA
									},
									new Task
									{
										Name = "Task D for userB",
										Owner = userB
									},
								}
							},
						}
					});
					session.SaveChanges();
				}

				int tasksForUserA = 3;
				int tasksForUserB = 1;
				using (var session = store.OpenSession())
				{
					var result = session.Query<TasksCount_ForPerson.Result, TasksCount_ForPerson>()
							.Customize(c => c.WaitForNonStaleResults())
							.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.Equal(tasksForUserA, result.Single(s => s.OwnerId == userA.Id).Count);
					Assert.Equal(tasksForUserB, result.Single(s => s.OwnerId == userB.Id).Count);
				}

				using (var session = store.OpenSession())
				{
					var project = session.Query<Project>().First();

					// I only change the name of the tasks, which should not cause the counts to go up
					project.Activities.ElementAt(0).Tasks.ElementAt(0).Name = "Touch task";
					project.Activities.ElementAt(0).Tasks.ElementAt(1).Name = "Touch task 2";
					project.Activities.ElementAt(1).Tasks.ElementAt(0).Name = "Touch Task 3";
					project.Activities.ElementAt(1).Tasks.ElementAt(1).Name = "Touch task 4";
					session.SaveChanges();
				}


				using (var session = store.OpenSession())
				{
					var result =
						session.Query<TasksCount_ForPerson.Result, TasksCount_ForPerson>().Customize(c => c.WaitForNonStaleResults()).
							ToList();

					// these two will fail.
					Assert.Equal(tasksForUserA, result.Single(s => s.OwnerId == userA.Id).Count);
					Assert.Equal(tasksForUserB, result.Single(s => s.OwnerId == userB.Id).Count);
				}
			}
		}
		public class TasksCount_ForPerson : AbstractIndexCreationTask<Project, TasksCount_ForPerson.Result>
		{
			public class Result
			{
				public string OwnerId { get; set; }
				public int Count { get; set; }
			}

			public TasksCount_ForPerson()
			{
				Map = projects => from project in projects
				                  from task in project.Activities.SelectMany(a => a.Tasks)
				                  select new
				                  {
				                  	OwnerId = task.Owner.Id,
				                  	Count = 1
				                  };

				Reduce =
					results => results
						.GroupBy(g => g.OwnerId)
						.Select(p => new
						{
							OwnerId = p.Select(r => r.OwnerId).First(),
							Count = p.Sum(result => result.Count)
						});
			}
		}

		public class Project
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public IList<Activity> Activities { get; set; }

			public Project()
			{
				Activities = new List<Activity>();
			}
		}

		public class Activity
		{
			public string Name { get; set; }
			public Person Owner { get; set; }
			public IList<Task> Tasks { get; set; }

			public Activity()
			{
				Tasks = new List<Task>();
			}
		}

		public class Task
		{
			public string Name { get; set; }
			public Person Owner { get; set; }
			public bool Done { get; set; }
		}

		public class Person
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}