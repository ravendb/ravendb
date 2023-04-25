using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Operations;
using SlowTests.MailingList;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Verifications
{
    public class NullDynamicValues : RavenTestBase
    {
        public NullDynamicValues(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void TransformerOperationWithNullDynamicValues(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                PerformTestWithNullDynamicValues(store);
            }
        }

        private void PerformTestWithNullDynamicValues(DocumentStore store)
        {
            // 1. Create document
            using (var session = store.OpenSession())
            {
                session.Store(new Person { Id = "persons/1" });
                session.SaveChanges();
            }

            string[] operations = new string[] { "+", "Plus", "-", "Minus", "%", "Percent", "*", "Mult", "/", "Divide" };
            for (int i = 0; i < operations.Length - 1; i += 2)
            {
                // 2. Define index with an operation on a field that will be null at run time
                var indexName = $"NullDynamicValueIndex{operations[i + 1]}Operation";

                var transformerDefinition = new IndexDefinition
                {
                    Name = indexName,
                    Maps =
                    {
                        $"from u in docs select new {{ Age = (u.Age {operations[i]} 1) ?? -3, u.Name }}"
                    }
                };

                store.Maintenance.Send(new PutIndexesOperation(transformerDefinition));

                Indexes.WaitForIndexing(store);

                // 3. Retrieve doc with transformer & verify 'Age' is null since it is not part of Person
                using (var session = store.OpenSession())
                {
                    WaitForUserToContinueTheTest(store);
                    var docs = session.Advanced.DocumentQuery<dynamic>(indexName)
                        .WhereEquals("Age", -3)
                        .ToList();
                    Assert.NotEmpty(docs);
                }
            }
        }
    }
}
