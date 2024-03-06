using System;
using System.Linq;
using Corax.Querying.Matches.SortingMatches;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21998 : RavenTestBase
{
    private const int NumberOfDocsToPut = 5000;
    public RavenDB_21998(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AssertNoSortingWhenThereAreNoTermsInOrderByField(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            PrepareData(store, NumberOfDocsToPut);
            
            using (var session = store.OpenSession())
            {
                QueryTimings timings = null;
                
                var result = session.Query<Question, DummyIndex>().Customize(x => x.Timings(out timings)).Where(x => x.Community == "SomeCommunity").OrderByDescending(x => x.CreatedAt).ToList();
                
                Assert.IsType<QueryInspectionNode>(timings.QueryPlan);
                
                Assert.DoesNotContain("sorting", ((QueryInspectionNode)timings.QueryPlan).Operation, StringComparison.InvariantCultureIgnoreCase);
                Assert.Equal(NumberOfDocsToPut, result.Count);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AssertOnlyFieldsWithNoTermsAreSkipped(Options options)
    {
        const int numberOfDocsToPut = 5000;
        
        using (var store = GetDocumentStore(options))
        {
            PrepareData(store, numberOfDocsToPut);

            using (var session = store.OpenSession())
            {
                QueryTimings timings = null;
                
                var result = session.Query<Question, DummyIndex>().Customize(x => x.Timings(out timings)).Where(x => x.Community == "SomeCommunity").OrderBy(x => x.SomeValue).ThenBy(x => x.CreatedAt).ToList();
                
                Assert.IsType<QueryInspectionNode>(timings.QueryPlan);
                
                Assert.DoesNotContain(nameof(SortingMultiMatch), ((QueryInspectionNode)timings.QueryPlan).Operation, StringComparison.InvariantCultureIgnoreCase);
                Assert.Contains(nameof(SortingMatch), ((QueryInspectionNode)timings.QueryPlan).Operation, StringComparison.InvariantCultureIgnoreCase);
                
                Assert.Equal(NumberOfDocsToPut, result.Count);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AssertOnlyFieldsWithNoTermsAreSkippedInSharding(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            PrepareData(store, NumberOfDocsToPut);
            
            using (var session = store.OpenSession())
            {
                var result = session.Query<Question, DummyIndex>().Where(x => x.Community == "SomeCommunity").OrderByDescending(x => x.CreatedAt).ThenByDescending(x => x.SomeValue).ToList();
                
                Assert.Equal(Enumerable.Range(1, 5000).Reverse(), result.Select(x => x.SomeValue));
                Assert.Equal(NumberOfDocsToPut, result.Count);
            }
        }
    }

    private void PrepareData(IDocumentStore store, int numberOfDocumentsToInsert)
    {
        using (var session = store.OpenSession())
        {
            var q1 = new Question() { Community = "SomeCommunity", SomeValue = 1 };
            
            session.Store(q1);
            
            using (BulkInsertOperation bulkInsert = store.BulkInsert())
            {
                for (int i = 2; i < numberOfDocumentsToInsert + 1; i++)
                {
                    bulkInsert.Store(new Question()
                    {
                        Id = $"questions/{i}${q1.Id}",
                        Community = "SomeCommunity",
                        SomeValue = i
                    });
                }
            }
            
            session.SaveChanges();
            
            session.Advanced.DocumentStore.Operations.ForDatabase(session.Advanced.DocumentStore.Database).Send(new PatchByQueryOperation(
                """
                from Questions
                update
                {
                    delete this.CreatedAt;
                }
                """)).WaitForCompletion(TimeSpan.FromSeconds(30));

            var index = new DummyIndex();

            index.Execute(store);

            Indexes.WaitForIndexing(store);
        }
    }
    
    private class Question
    {
        public string Id { get; set; }
        public string Community { get; set; }
        public DateTime CreatedAt { get; set; }
        public int SomeValue { get; set; }
    }

    private class DummyIndex : AbstractIndexCreationTask<Question>
    {
        public DummyIndex()
        {
            Map = questions => from question in questions
                select new
                {
                    question.Community,
                    question.CreatedAt,
                    question.SomeValue
                };
        }
    }
}
