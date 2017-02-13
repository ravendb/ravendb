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
            const string name = "Test";
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Maps = { "from d in docs select new {d.Name}" }
                };

                var transformerDefinition = new TransformerDefinition
                {
                    Name = name,
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                };

                store.Admin.Send(new PutIndexOperation(name, indexDefinition));
                Assert.NotNull(store.Admin.Send(new GetIndexOperation(name)));
                var e = Assert.Throws<RavenException>(() => store.Admin.Send(new PutTransformerOperation(name, transformerDefinition)));
                Assert.Contains($"Tried to create a transformer with a name of {name}, but an index under the same name exist", e.Message);
            }
        }

        [Fact]
        public void CanCreateTransformerWithTheSameNameOfDeletedIndex()
        {
            const string name = "Test";
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Maps = { "from d in docs select new {d.Name}" }
                };

                var transformerDefinition = new TransformerDefinition
                {
                    Name = name,
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                };

                store.Admin.Send(new PutIndexOperation(name, indexDefinition));

                Assert.NotNull(store.Admin.Send(new GetIndexOperation(name)));
                store.Admin.Send(new DeleteIndexOperation(name));
                Assert.Null(store.Admin.Send(new GetIndexOperation(name)));

                store.Admin.Send(new PutTransformerOperation(name, transformerDefinition));
                Assert.NotNull(store.Admin.Send(new GetTransformerOperation(name)));

            }
        }
    }
}
