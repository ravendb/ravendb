using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Client.Commands;
using Raven.Client.Data;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Basic
{
    public class Crud : RavenNewTestBase
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
                    var user = await session.LoadAsync<User>("users/1");
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
                var requestExecuter = store.GetRequestExecuter();

                JsonOperationContext context;
                using (requestExecuter.ContextPool.AllocateOperationContext(out context))
                {
                    var djv = new DynamicJsonValue
                    {
                        ["Name"] = "Fitzchak",
                        ["LastName"] = "Very big value here, so can reproduce the issue",
                        [Constants.Metadata.Key] = new DynamicJsonValue
                        {
                            ["SomeMoreData"] = "Make this object bigger",
                            ["SomeMoreData2"] = "Make this object bigger",
                            ["SomeMoreData3"] = "Make this object bigger"
                        }
                    };

                    var json = context.ReadObject(djv, "users/1");

                    var putCommand = new PutDocumentCommand
                    {
                        Context = context,
                        Id = "users/1",
                        Document = json
                    };

                    requestExecuter.Execute(putCommand, context);

                    djv = new DynamicJsonValue
                    {
                        ["Name"] = "Fitzchak"
                    };

                    json = context.ReadObject(djv, "users/1");

                    putCommand = new PutDocumentCommand
                    {
                        Context = context,
                        Id = "users/1",
                        Document = json
                    };

                    requestExecuter.Execute(putCommand, context);
                }
            }
        }
    }
}
