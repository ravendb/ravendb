using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.JsonPatch;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using PatchRequest = Raven.Client.Documents.Operations.PatchRequest;

namespace FastTests.Client;

public class BatchCommandWithNoReplyFlagTest : RavenTestBase
{
    public BatchCommandWithNoReplyFlagTest(ITestOutputHelper output) : base(output)
    {
    }
        
    [RavenFact(RavenTestCategory.Patching)]
    public async Task BatchWithNoReply_WhenPatch_ShouldNotFail()
    {
        const string user1Id = "users/1";
        const string user1Value = "SomeValue";
        
        var store = GetDocumentStore();
        var requestExecutor = store.GetRequestExecutor();
        
        using (var session = store.OpenSession())
        using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            session.Store(new TestObj(), user1Id);
            session.SaveChanges();
                
            var result = new InMemoryDocumentSessionOperations.SaveChangesData((InMemoryDocumentSessionOperations)session);
            var patchRequest = new PatchRequest{Script = "this.Name = args.val_0;", Values = new Dictionary<string, object>{{"val_0", user1Value}}};
            result.SessionCommands.Add(new PatchCommandData(user1Id, null, patchRequest));
            
            var sbc = new TestSingleNodeBatchCommand(DocumentConventions.Default, context, result.SessionCommands, result.Options);
            
            await requestExecutor.ExecuteAsync(sbc, context);
        }

        using (var session = store.OpenSession())
        {
            var user = session.Load<TestObj>(user1Id);
            Assert.Equal(user1Value, user.Name);
        }
    }
    
    [RavenFact(RavenTestCategory.Patching)]
    public async Task BatchWithNoReply_WhenJsonPatch_ShouldNotFail()
    {
        const string user2Id = "users/1";
        const string user2Value = "SomeValue";
        
        var store = GetDocumentStore();
        var requestExecutor = store.GetRequestExecutor();
        
        using (var session = store.OpenSession())
        using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            session.Store(new TestObj(), user2Id);
            session.SaveChanges();
                
            var result = new InMemoryDocumentSessionOperations.SaveChangesData((InMemoryDocumentSessionOperations)session);
            var jpd = new JsonPatchDocument();
            jpd.Add("/Name", user2Value);
            result.SessionCommands.Add(new JsonPatchCommandData(user2Id, jpd));
            var sbc = new TestSingleNodeBatchCommand(DocumentConventions.Default, context, result.SessionCommands, result.Options);
            
            await requestExecutor.ExecuteAsync(sbc, context);
        }

        using (var session = store.OpenSession())
        {
            var user = session.Load<TestObj>(user2Id);
            Assert.Equal(user2Value, user.Name);
        }
    }
    
    private class TestObj
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
    
    private class TestSingleNodeBatchCommand : SingleNodeBatchCommand
    {
        public TestSingleNodeBatchCommand(DocumentConventions conventions, JsonOperationContext context, IList<ICommandData> commands, BatchOptions options = null, TransactionMode mode = TransactionMode.SingleNode) : base(conventions, context, commands, options, mode)
        {
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var httpRequestMessage = base.CreateRequest(ctx, node, out url);
            url += "noreply=true";
            return httpRequestMessage;
        }
    }
}
