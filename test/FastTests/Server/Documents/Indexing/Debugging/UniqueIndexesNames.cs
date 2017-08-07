using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Transformers;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Indexes;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Debugging
{
    public class UniqueIndexesNames : RavenTestBase
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

                store.Admin.Send(new PutTransformerOperation(new TransformerDefinition
                {
                    Name = "Test",
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                }));

                Assert.NotNull(store.Admin.Send(new GetTransformerOperation("Test")));
                var e = Assert.Throws<IndexOrTransformerAlreadyExistException>(() => store.Admin.Send(new PutIndexesOperation(indexDefinition)));
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

                store.Admin.Send(new PutTransformerOperation(new TransformerDefinition
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
