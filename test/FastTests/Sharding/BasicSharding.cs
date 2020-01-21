using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    public class BasicSharding : ShardedTestBase
    {
        public BasicSharding(ITestOutputHelper output) : base(output)
        {
        }

        public class User
        {
            public string Name;
        }

        [Fact]
        public void CanCreateShardedDatabase()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/1");
                    Assert.Null(u);
                }
                
            }
        }

        [Fact]
        public void CanPutAndGetItem()
        {
            using (var store = GetShardedDocumentStore())
            {
                PutUser(store, new DynamicJsonValue { ["Name"] = "Oren" }, "users/1");

                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/1");
                    Assert.NotNull(u);
                    Assert.Equal("Oren", u.Name);
                }
            }
        }

        private static void PutUser(IDocumentStore store, DynamicJsonValue user, string id)
        {
            RequestExecutor requestExecutor = store.GetRequestExecutor();
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var blittableJsonReaderObject = context.ReadObject( user, id);
                requestExecutor.Execute(new PutDocumentCommand(id, null, blittableJsonReaderObject), context);
            }
        }

        [Fact]
        public void CanPutAndGetMultipleItems()
        {
            using (var store = GetShardedDocumentStore())
            {
                PutUser(store, new DynamicJsonValue { ["Name"] = "Oren" }, "users/1");
                PutUser(store, new DynamicJsonValue { ["Name"] = "Tal" }, "users/2");
                PutUser(store, new DynamicJsonValue { ["Name"] = "Maxim" }, "users/3");
                using (var s = store.OpenSession())
                {
                    var users = s.Load<User>(new []{ "users/1" , "users/2", "users/3" });
                    Assert.NotNull(users);
                    Assert.Equal(3, users.Count);
                    Assert.Equal("Oren", users["users/1"].Name);
                    Assert.Equal("Tal", users["users/2"].Name);
                    Assert.Equal("Maxim", users["users/3"].Name);
                }
            }
        }
    }
}
