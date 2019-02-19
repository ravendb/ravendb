using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8710:RavenTestBase
    {
        [Fact]
        public void CanOverwrite()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {                    
                    session.Store(new Person
                    {
                        Name = "John Doe"
                    }, "people/1");
                    
                    session.SaveChanges();
                }                

                var reqEx = store.GetRequestExecutor();

                using (reqEx.ContextPool.AllocateOperationContext(out var context))
                {
                    PatchOperation operation = new PatchOperation(
                        id: "people/1",
                        changeVector: null,
                        patch: new PatchRequest
                        {
                        Script = @"this.NewField= 'new value';
                        console.log('foo')"
                        },
                        patchIfMissing: null);

                    var cmd = operation.GetCommand(store, store.Conventions, context, reqEx.Cache, true, false);
                    store.Commands().Execute(cmd);
                    Assert.Equal("foo", (cmd.Result.Debug["Info"] as BlittableJsonReaderArray)[0].ToString());
                }
            }
        }
    }
}
