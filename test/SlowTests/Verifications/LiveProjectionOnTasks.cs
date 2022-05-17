using Tests.Infrastructure;
using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Verifications
{
    public class LiveProjectionOnTasks : RavenTestBase
    {
        public LiveProjectionOnTasks(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void TaskLiveProjection(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                new TaskSummaryIndex().Execute((IDocumentStore)documentStore);

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
                    var optChaining = options.JavascriptEngineMode.ToString() == "Jint" ? "" : "?";
                    
                    var results = session.Advanced
                        .RawQuery<dynamic>(@$"
declare function fetch(r){{
    var g = load('users/'  + r.GiverId);
    var t = load('users/'  + r.TakerId);
    var p = load('places/' + r.PlaceId)
                                         
    return {{
        Id: id(r),
        Description: r.Description,
        Start: r.Start,
        End: r.End,
        GiverId: r.GiverId,
        GiverName: g{optChaining}.Name,
        TakerId: r.TakerId,
        TakerName: r.Name,
        PlaceId: r.PlaceId,
        PlaceName: p{optChaining}.Name
    }};
}}
from index TaskSummaryIndex as r
select fetch(r)
")
                        .WaitForNonStaleResults()
                        .ToList();

                    var first = results.FirstOrDefault();

                    Assert.NotNull(first);
                    Assert.Equal((string)first.Id, "tasks/1-A");
                    Assert.Equal((int)first.GiverId, 1);
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
