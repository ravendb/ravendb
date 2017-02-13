using Raven.Client.Exceptions;
using Raven.Client.Indexing;
using Raven.Client.Operations.Databases.Indexes;
using Raven.Client.Operations.Databases.Transformers;
using Xunit;

namespace FastTests.Server.Documents.Transformers
{
    public class UniqueTransformersNames : RavenNewTestBase
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
                var e = Assert.Throws<RavenException>(() => store.Admin.Send(new PutTransformerOperation("Test", transformerDefinition)));
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

                store.Admin.Send(new PutTransformerOperation("Test", transformerDefinition));
                Assert.NotNull(store.Admin.Send(new GetTransformerOperation("Test")));

            }
        }
    }
}
