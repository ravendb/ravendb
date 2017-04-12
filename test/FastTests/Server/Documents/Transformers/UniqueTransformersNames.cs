using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace FastTests.Server.Documents.Transformers
{
    public class UniqueTransformersNames : RavenTestBase
    {
        [Fact]
        public void TransformersAndIndexesNameShouldBeUniqe()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Maps = { "from d in docs select new {d.Name}" },
                    Name = "Test"
                };

                var transformerDefinition = new TransformerDefinition
                {
                    Name = "Test",
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                };

                store.Admin.Send(new PutIndexesOperation(indexDefinition));
                Assert.NotNull(store.Admin.Send(new GetIndexOperation("Test")));
                var e = Assert.Throws<IndexOrTransformerAlreadyExistException>(() => store.Admin.Send(new PutTransformerOperation(transformerDefinition)));
                Assert.Contains($"Tried to create a transformer with a name of {"Test"}, but an index under the same name exist", e.Message);
            }
        }

        [Fact]
        public void CanCreateTransformerWithTheSameNameOfDeletedIndex()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Maps = { "from d in docs select new {d.Name}" },
                    Name = "Test"
                };

                var transformerDefinition = new TransformerDefinition
                {
                    Name = "Test",
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                };

                store.Admin.Send(new PutIndexesOperation(indexDefinition));

                Assert.NotNull(store.Admin.Send(new GetIndexOperation("Test")));
                store.Admin.Send(new DeleteIndexOperation("Test"));
                Assert.Null(store.Admin.Send(new GetIndexOperation("Test")));

                store.Admin.Send(new PutTransformerOperation(transformerDefinition));
                Assert.NotNull(store.Admin.Send(new GetTransformerOperation("Test")));

            }
        }
    }
}
