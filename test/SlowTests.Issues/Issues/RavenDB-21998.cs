using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_21998 : RavenTestBase
{
    private const int NumberOfDocsToPut = 5000;
    public RavenDB_21998(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void OrderByFieldWithNoTermsReturnsAllDocs(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            PrepareData(store, NumberOfDocsToPut);

            using (var session = store.OpenSession())
            {
                // CreatedAt has no terms (every value was patched away), so ordering by it is a uniform
                // "missing" key: the order between docs is unspecified, but no doc may be dropped
                // (RavenDB-25281 keeps the empty sort slot and sorts via InMemorySort).
                var result = session.Query<Question, DummyIndex>().Where(x => x.Community == "SomeCommunity").OrderByDescending(x => x.CreatedAt).ToList();

                Assert.Equal(NumberOfDocsToPut, result.Count);
                Assert.Equal(NumberOfDocsToPut, result.Select(x => x.Id).Distinct().Count());
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void MultiKeySortWithTrailingEmptyFieldOrdersByLeadingKey(Options options)
    {
        const int numberOfDocsToPut = 5000;

        using (var store = GetDocumentStore(options))
        {
            PrepareData(store, numberOfDocsToPut);

            using (var session = store.OpenSession())
            {
                // SomeValue has terms; CreatedAt has none. The trailing empty key never breaks a tie, so the
                // result must be fully ordered by the leading SomeValue key, with every doc surviving.
                var result = session.Query<Question, DummyIndex>().Where(x => x.Community == "SomeCommunity").OrderBy(x => x.SomeValue).ThenBy(x => x.CreatedAt).ToList();

                Assert.Equal(NumberOfDocsToPut, result.Count);
                Assert.Equal(Enumerable.Range(1, NumberOfDocsToPut), result.Select(x => x.SomeValue));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void MultiKeySortWithLeadingEmptyFieldFallsBackToTieBreakInSharding(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            PrepareData(store, NumberOfDocsToPut);

            using (var session = store.OpenSession())
            {
                // CreatedAt (leading key) is empty for every doc, so the order is decided entirely by the
                // SomeValue tie-break key — the sharded local-sort/merge must produce the same surviving set.
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
