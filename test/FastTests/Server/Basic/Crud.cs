using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Basic
{
    public class Crud : RavenTestBase
    {
        [Fact]
        public async Task CanSaveAndLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" }, "users/arek");

                    await session.SaveChangesAsync();

                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1-A");
                    Assert.NotNull(user);
                    Assert.Equal("Fitzchak", user.Name);

                    user = await session.LoadAsync<User>("users/arek");
                    Assert.NotNull(user);
                    Assert.Equal("Arek", user.Name);
                }
            }
        }

        [Fact]
        public void CanOverwriteDocumentWithSmallerValue()
        {
            using (var store = GetDocumentStore())
            {
                var requestExecuter = store.GetRequestExecutor();

                using (requestExecuter.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var djv = new DynamicJsonValue
                    {
                        ["Name"] = "Fitzchak",
                        ["LastName"] = "Very big value here, so can reproduce the issue",
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            ["SomeMoreData"] = "Make this object bigger",
                            ["SomeMoreData2"] = "Make this object bigger",
                            ["SomeMoreData3"] = "Make this object bigger"
                        }
                    };

                    var json = context.ReadObject(djv, "users/1");

                    var putCommand = new PutDocumentCommand("users/1", null, json);

                    requestExecuter.Execute(putCommand, context);

                    djv = new DynamicJsonValue
                    {
                        ["Name"] = "Fitzchak"
                    };

                    json = context.ReadObject(djv, "users/1");

                    putCommand = new PutDocumentCommand("users/1", null, json);

                    requestExecuter.Execute(putCommand, context);
                }
            }
        }
    }
}
