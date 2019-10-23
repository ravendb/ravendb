using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Queries.Dynamic.MapReduce
{
    public class RavenDB_7044 : RavenTestBase
    {
        public RavenDB_7044(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_filter_by_property_of_composite_group_by_key()
        {
            using (var store = GetDocumentStore())
            {
                var today = DateTime.UtcNow;
                var tomorrow = today.AddDays(1);

                using (var session = store.OpenSession())
                {
                    session.Store(new ToDoTask()
                    {
                        DueDate = today,
                        Completed = false
                    });

                    session.Store(new ToDoTask
                    {
                        DueDate = today,
                        Completed = true
                    });

                    session.Store(new ToDoTask
                    {
                        DueDate = tomorrow,
                        Completed = false
                    });

                    session.Store(new ToDoTask
                    {
                        DueDate = tomorrow,
                        Completed = false
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tasksPerDay =
                    (from t in session.Query<ToDoTask>()
                        where t.Completed == false
                        group t by new { t.DueDate, t.Completed }
                        into g
                        select new
                        {
                            g.Key.DueDate,
                            TasksPerDate = g.Count()
                        }).ToList();

                    Assert.Equal(2, tasksPerDay.Count);

                    tasksPerDay = tasksPerDay.OrderBy(x => x.TasksPerDate).ToList();

                    Assert.Equal(today, tasksPerDay[0].DueDate);
                    Assert.Equal(1, tasksPerDay[0].TasksPerDate);

                    Assert.Equal(tomorrow, tasksPerDay[1].DueDate);
                    Assert.Equal(2, tasksPerDay[1].TasksPerDate);
                }

                var indexDefinitions = store.Maintenance.Send(new GetIndexesOperation(0, 10));

                Assert.Equal(1, indexDefinitions.Length); // all of the above queries should be handled by the same auto index
                Assert.Equal("Auto/ToDoTasks/ByCountReducedByCompletedAndDueDate", indexDefinitions[0].Name);
            }
        }

        private class ToDoTask
        {
            public DateTime DueDate { get; set; }

            public bool Completed { get; set; }
        }
    }
}
