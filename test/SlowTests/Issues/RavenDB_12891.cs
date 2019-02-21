using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12891 : RavenTestBase
    {
        [Fact]
        public void ShouldDeleteAllArtificialDocuments()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Name = "PartialResourceStateTimelinesIndex",
                    Maps = { @"from r in docs.PartialResourceStates
select new
{
  r.resource,
  states = new[] { r }
}" },
                    Reduce = @"from result in results
group result by result.resource into g
select new {
  resource = g.Key,
  states = g.SelectMany(x => x.states).OrderBy(x => x.timestamp)
}",
                    OutputReduceToCollection = "PartialResourceStateTimelines"
                }}));

                int numberOfDocs = 1500;
                int numberOfMapReduceResults = 10;

                var resources = new List<resource>();

                for (int i = 0; i < numberOfMapReduceResults; i++)
                {
                    resources.Add(new resource()
                    {
                        id = Guid.NewGuid(),
                        scope = new scope()
                        {
                            localCustomerId = $"ids/{i}",
                            localProjectId = Guid.NewGuid(),
                            location = $"locations/{i}",
                            platformInstance = $"platforms/{i}",
                            platformType = $"platformTypes/{i}"
                        }
                    });
                }

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < numberOfDocs; i++)
                    {
                        resource resource = resources[i % resources.Count];
                        bulk.Store(new PartialResourceStates()
                        {
                            resource = resource,
                            created = DateTimeOffset.Now,
                            displayName = i % 100,
                            terminated = null,
                            timestamp = DateTimeOffset.Now,
                            traits = new traits()
                            {
                                disk = i % 10,
                                diskEphemeral = i % 100,
                            }
                        }, $"PartialResourceStates/{i}");
                    }
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(numberOfMapReduceResults, session.Query<dynamic>("PartialResourceStateTimelinesIndex").Count());

                    int countOfArtificialDocuments = session.Advanced.RawQuery<dynamic>("from PartialResourceStateTimelines").Count();

                    Assert.Equal(numberOfMapReduceResults, countOfArtificialDocuments);
                }

                store.Maintenance.Send(new StopIndexOperation("PartialResourceStateTimelinesIndex"));

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < numberOfDocs; i++)
                    {
                        session.Delete($"PartialResourceStates/{i}");
                    }

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StartIndexOperation("PartialResourceStateTimelinesIndex"));

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Query<dynamic>("PartialResourceStateTimelinesIndex").Count());

                    int countOfArtificialDocuments = session.Advanced.RawQuery<dynamic>("from PartialResourceStateTimelines").Count();

                    Assert.Equal(0, countOfArtificialDocuments);
                }
            }
        }

        private class scope
        {
            public string localCustomerId { get; set; }
            public Guid localProjectId { get; set; }
            public string location { get; set; }
            public string platformInstance { get; set; }
            public string platformType { get; set; }
        }

        private class resource
        {
            public Guid id { get; set; }
            public scope scope { get; set; }
            public string type { get; set; }
        }

        private class traits
        {
            public int disk { get; set; }
            public int diskEphemeral { get; set; }
            public int diskRoot { get; set; }
            public string flavor { get; set; }
            public int ramMb { get; set; }
            public string state { get; set; }
            public int vcpu { get; set; }
        }

        private class PartialResourceStates
        {
            public DateTimeOffset created { get; set; }
            public int displayName { get; set; }
            public resource resource { get; set; }
            public object terminated { get; set; }
            public DateTimeOffset timestamp { get; set; }
            public traits traits { get; set; }
        }
    }
}
