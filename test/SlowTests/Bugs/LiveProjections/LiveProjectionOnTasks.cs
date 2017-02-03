using System;
using System.Linq;
using FastTests;
using Raven.NewClient.Abstractions;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Indexes;
using Xunit;

namespace SlowTests.Bugs.LiveProjections
{
    public class LiveProjectionOnTasks : RavenNewTestBase
    {
        [Fact]
        public void TaskLiveProjection()
        {
            using (var documentStore = GetDocumentStore())
            {
                new TaskSummaryIndex().Execute((IDocumentStore)documentStore);
                new TaskSummaryTransformer().Execute((IDocumentStore)documentStore);

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
                        .TransformWith<TaskSummaryTransformer, dynamic>()
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


        private class User
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        private class TaskSummary
        {
            public string Id { get; set; }

            public string Description { get; set; }

            public DateTime Start { get; set; }

            public DateTime End { get; set; }

            public int GiverId { get; set; }

            public string GiverName { get; set; }

            public int TakerId { get; set; }

            public string TakerName { get; set; }

            public int PlaceId { get; set; }

            public string PlaceName { get; set; }
        }

        private class TaskSummaryIndex : AbstractIndexCreationTask<Task, TaskSummary>
        {
            public TaskSummaryIndex()
            {
                Map = docs => from t in docs
                              select new { t.Start };

                IndexSortOptions.Add(s => s.Start, SortOptions.String);
            }
        }

        private class TaskSummaryTransformer : AbstractTransformerCreationTask<Task>
        {
            public TaskSummaryTransformer()
            {

                TransformResults = results => from result in results
                                              let giver = LoadDocument<User>("users/" + result.GiverId)
                                              let taker = LoadDocument<User>("users/" + result.TakerId)
                                              let place = LoadDocument<Place>("places/" + result.PlaceId)
                                              select new
                                              {
                                                  Id = result.Id,
                                                  Description = result.Description,
                                                  Start = result.Start,
                                                  End = result.End,
                                                  GiverId = result.GiverId,
                                                  GiverName = giver.Name,
                                                  TakerId = result.TakerId,
                                                  TakerName = taker != null ? taker.Name : null,
                                                  PlaceId = result.PlaceId,
                                                  PlaceName = place.Name
                                              };
            }
        }

        private class Task
        {
            public string Id { get; set; }

            public string Description { get; set; }

            public DateTime Start { get; set; }

            public DateTime End { get; set; }

            public int GiverId { get; set; }

            public int TakerId { get; set; }

            public int PlaceId { get; set; }
        }

        private class Place
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }
    }
}
