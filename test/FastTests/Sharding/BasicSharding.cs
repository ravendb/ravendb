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
                RequestExecutor requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
                {
                    var blittableJsonReaderObject = context.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "Oren"
                    },"users/1");
                    requestExecutor.Execute(new PutDocumentCommand("users/1", null, blittableJsonReaderObject), context);
                }


                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/1");
                    Assert.NotNull(u);
                    Assert.Equal("Oren", u.Name);
                }
            }
        }
    }
}
