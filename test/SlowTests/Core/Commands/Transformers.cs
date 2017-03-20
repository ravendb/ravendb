using System.Threading.Tasks;

using FastTests;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.Core.Commands
{
    public class Transformers : RavenTestBase
    {
        [Fact]
        public async Task CanPutUpdateAndDeleteTransformer()
        {
            using (var store = GetDocumentStore())
            {
                const string usersSelectNames = "users/selectName";

                await store.Admin.SendAsync(new PutTransformerOperation(new TransformerDefinition
                {
                    Name = usersSelectNames,
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                }));

                await store.Admin.SendAsync(new PutTransformerOperation(new TransformerDefinition
                {
                    Name = usersSelectNames,
                    TransformResults = "from user in results select new { Name = user.Name }"
                }));

                var transformer = await store.Admin.SendAsync(new GetTransformerOperation(usersSelectNames));
                Assert.Equal("from user in results select new { Name = user.Name }", transformer.TransformResults);

                var transformers = await store.Admin.SendAsync(new GetTransformersOperation(0, 5));
                Assert.Equal(1, transformers.Length);

                await store.Admin.SendAsync(new DeleteTransformerOperation(usersSelectNames));
                Assert.Null(await store.Admin.SendAsync(new GetTransformerOperation(usersSelectNames)));
            }
        }
    }
}
