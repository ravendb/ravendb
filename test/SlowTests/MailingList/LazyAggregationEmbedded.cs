// -----------------------------------------------------------------------
//  <copyright file="LazyAggregationEmbedded.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class LazyAggregationEmbedded : RavenTestBase
    {
        public LazyAggregationEmbedded(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Test()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())

                {
                    session.Store(new Task { AssigneeId = "users/1", Id = "tasks/1" });

                    session.Store(new Task { AssigneeId = "users/1", Id = "tasks/2" });


                    session.Store(new Task { AssigneeId = "users/2", Id = "tasks/3" });


                    session.SaveChanges();

                    new TaskIndex().Execute(store);

                    Indexes.WaitForIndexing(store);

                    var query = session.Query<Task, TaskIndex>()
                        .AggregateBy(factory => factory.ByField(t => t.AssigneeId).WithDisplayName("AssigneeId"));

                    var lazyOperation = query.ExecuteLazy(); // blows up here

                    var facetValue = lazyOperation.Value;

                    var userStatistics = facetValue["AssigneeId"].Values.ToDictionary(v => v.Range, v => v.Count);

                    Assert.Equal(2, userStatistics["users/1"]);

                    Assert.Equal(1, userStatistics["users/2"]);
                }
            }
        }

        private class TaskIndex : AbstractIndexCreationTask<Task>
        {
            public TaskIndex()
            {
                Map = tasks =>
                    from task in tasks
                    select new { task.AssigneeId };
            }
        }

        private class Task

        {
            public string Id { get; set; }
            public string AssigneeId { get; set; }
        }
    }
}
