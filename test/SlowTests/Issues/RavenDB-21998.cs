using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21998 : RavenTestBase
{
    public RavenDB_21998(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void AssertNoSortingWhenThereAreNoTermsInOrderByField()
    {
        const int numberOfDocsToPut = 5000;

        using (var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax)))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Question()
                {
                    Community = "SomeCommunity"
                });
                
                session.SaveChanges();
                
                session.Advanced.DocumentStore.Operations.ForDatabase(session.Advanced.DocumentStore.Database).Send(new PatchByQueryOperation(
                    """
                    from Questions
                    update
                    {
                        delete this.CreatedAt;
                        for (var i = 0; i < 4999; ++i) {
                            put(id(this)+i, this);
                        }
                    }
                    """)).WaitForCompletion(TimeSpan.FromSeconds(30));
                
                var index = new DummyIndex();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);

                QueryTimings timings = null;
                
                var result = session.Query<Question, DummyIndex>().Customize(x => x.Timings(out timings)).Where(x => x.Community == "SomeCommunity").OrderByDescending(x => x.CreatedAt).ToList();
                
                Assert.IsType<QueryInspectionNode>(timings.QueryPlan);
                
                Assert.DoesNotContain("sorting", ((QueryInspectionNode)timings.QueryPlan).Operation, StringComparison.InvariantCultureIgnoreCase);
                Assert.Equal(numberOfDocsToPut, result.Count);
            }
        }
    }
    
    private class Question
    {
        public string Id { get; set; }
        public string Community { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    private class DummyIndex : AbstractIndexCreationTask<Question>
    {
        public DummyIndex()
        {
            Map = questions => from question in questions
                select new
                {
                    question.Community,
                    question.CreatedAt
                };
        }
    }
}
