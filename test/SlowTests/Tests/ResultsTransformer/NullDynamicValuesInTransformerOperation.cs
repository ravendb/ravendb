using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Transformers;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Tests.ResultsTransformer
{
    public class NullDynamicValuesInTransformerOperation : RavenTestBase
    {
        public class Person
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void TransformerOperationWithNullDynamicValues()
        {
            using (var store = GetDocumentStore())
            {
                PerformTestWithNullDynamicValues(store);
            }
        }
        
        private void PerformTestWithNullDynamicValues(IDocumentStore store)
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
                // 2. Define transformer with an operation on a field that will be null at run time
                var transformerName = $"NullDynamicValueTransformer{operations[i + 1]}Operation";

                var transformerDefinition = new TransformerDefinition
                {
                    Name = transformerName,
                    TransformResults = $"from u in results select new {{ Age = u.Age {operations[i]} 1 }}"
                };

                store.Admin.Send(new PutTransformerOperation(transformerDefinition));

                // 3. Retrieve doc with transformer & verify 'Age' is null since it is not part of Person
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<BlittableJsonReaderObject>("persons/1", transformerName, null);
                    Assert.NotNull(doc);
                    Assert.Null(doc["Age"]);
                }
            }
        }
    }
}