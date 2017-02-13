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
            const string name = "Test";
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Maps = { "from d in docs select new {d.Name}" }
                };

                store.Admin.Send(new PutTransformerOperation(name, new TransformerDefinition
                {
                    Name = name,
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                }));

                Assert.NotNull(store.Admin.Send(new GetTransformerOperation(name)));
                var e = Assert.Throws<RavenException>(() => store.Admin.Send(new PutIndexOperation(name, indexDefinition)));
                Assert.Contains($"Tried to create an index with a name of {name}, but a transformer under the same name exist", e.Message);

            }
        }

        [Fact]
        public void CanCreateIndexWithTheSameNameOfDeletedTransformer()
        {
            const string name = "Test";
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Maps = { "from d in docs select new {d.Name}" }
                };

                store.Admin.Send(new PutTransformerOperation(name, new TransformerDefinition
                {
                    Name = name,
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                }));

                Assert.NotNull(store.Admin.Send(new GetTransformerOperation(name)));
                store.Admin.Send(new DeleteTransformerOperation(name));
                Assert.Null(store.Admin.Send(new GetTransformerOperation(name)));

                store.Admin.Send(new PutIndexOperation(name, indexDefinition));
                Assert.NotNull(store.Admin.Send(new GetIndexOperation(name)));

            }
        }
    }
}
