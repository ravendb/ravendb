using Raven.Abstractions.Indexing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.Commands
{
    public class Transformers : RavenCoreTestBase
    {
        [Fact]
        public async Task CanPutUpdateAndDeleteTransformer()
        {
            using (var store = GetDocumentStore())
            {
                const string usersSelectNames = "users/selectName";

                await store.AsyncDatabaseCommands.PutTransformerAsync(usersSelectNames, new TransformerDefinition()
                {
                    Name = usersSelectNames,
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                });

                await store.AsyncDatabaseCommands.PutTransformerAsync(usersSelectNames, new TransformerDefinition()
                {
                    Name = usersSelectNames,
                    TransformResults = "from user in results select new { Name = user.Name }"
                });

                var transformer = await store.AsyncDatabaseCommands.GetTransformerAsync(usersSelectNames);
                Assert.Equal("from user in results select new { Name = user.Name }", transformer.TransformResults);

                var transformers = await store.AsyncDatabaseCommands.GetTransformersAsync(0, 5);
                Assert.Equal(1, transformers.Length);

                await store.AsyncDatabaseCommands.DeleteTransformerAsync(usersSelectNames);
                Assert.Null(await store.AsyncDatabaseCommands.GetTransformerAsync(usersSelectNames));
            }
        }
    }
}
