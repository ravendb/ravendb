using Raven.Client.Exceptions;
using Raven.Client.Indexing;
using Raven.Client.Operations.Databases.Indexes;
using Raven.Client.Operations.Databases.Transformers;
using Xunit;

namespace FastTests.Indexes
{
    public class UniqueIndexesNames : RavenNewTestBase
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

                store.Admin.Send(new PutTransformerOperation("Test", new TransformerDefinition
                {
                    Name = "Test",
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                }));

                Assert.NotNull(store.Admin.Send(new GetTransformerOperation("Test")));
                var e = Assert.Throws<RavenException>(() => store.Admin.Send(new PutIndexesOperation(indexDefinition)));
                Assert.Contains($"Tried to create an index with a name of Test, but a transformer under the same name exist", e.Message);

            }
        }

        [Fact]
        public void CanCreateIndexWithTheSameNameOfDeletedTransformer()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Maps = { "from d in docs select new {d.Name}" },
                    Name = "Test",
                };

                store.Admin.Send(new PutTransformerOperation("Test", new TransformerDefinition
                {
                    Name = "Test",
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                }));

                Assert.NotNull(store.Admin.Send(new GetTransformerOperation("Test")));
                store.Admin.Send(new DeleteTransformerOperation("Test"));
                Assert.Null(store.Admin.Send(new GetTransformerOperation("Test")));

                store.Admin.Send(new PutIndexesOperation(indexDefinition));
                Assert.NotNull(store.Admin.Send(new GetIndexOperation("Test")));

            }
        }
    }
}
