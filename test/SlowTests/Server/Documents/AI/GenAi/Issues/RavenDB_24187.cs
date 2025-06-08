using System;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;

public class RavenDB_24187(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task TestConvertingSampleObjectToSchema(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var sampleJson = new DynamicJsonValue
        {
            ["IsSpam"] = true, 
            ["Reason"] = "Concise reason for why this comment was marked as spam or harmful"
        };

        string schema;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            var blittableSample = context.ReadObject(sampleJson, "sample");

            var command = new ToJsonSchemaCommand(store.Conventions, blittableSample);
            await store.GetRequestExecutor().ExecuteAsync(command, context);

            var schemaResult = command.Result;
            Assert.True(schemaResult.TryGet("Result", out schema));
            Assert.False(string.IsNullOrWhiteSpace(schema));
            Assert.Contains("IsSpam", schema, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Reason", schema, StringComparison.OrdinalIgnoreCase);
        }

        var etl = Etl.WaitForEtlToComplete(store);

        config.JsonSchema = schema;

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if (idx < 0)
    return;
this.Comments[idx].IsSpamComment =  $output.IsSpam;
";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(config));

        const string id = "posts/1";
        using (var session = store.OpenSession())
        {
            var p = new GenAiBasics.Post(
                [
                    new GenAiBasics.Comment("Legit comment ", "aviv"),
                    new GenAiBasics.Comment("Spam comment", "evil bot"),
                    new GenAiBasics.Comment("Harmful content", "racist bot")
                ], "I, pencil", "A B52 pencil...");
            session.Store(p, id);
            session.SaveChanges();
        }

        Assert.True(etl.Wait(TimeSpan.FromSeconds(30)));

        using (var session = store.OpenSession())
        {
            var post = session.Load<BlittableJsonReaderObject>(id);
            Assert.NotNull(post);

            Assert.True(post.TryGet("Comments", out BlittableJsonReaderArray comments));
            Assert.Equal(3, comments.Length);

            foreach (var o in comments)
            {
                var comment = o as BlittableJsonReaderObject;
                Assert.NotNull(comment);

                Assert.True(comment.TryGet("IsSpamComment", out bool _));
            }

        }
    }


    private class ToJsonSchemaCommand : RavenCommand<BlittableJsonReaderObject>
    {
        private readonly DocumentConventions _conventions;
        private readonly BlittableJsonReaderObject _sample;
        public override bool IsReadRequest => false;

        public ToJsonSchemaCommand(DocumentConventions conventions, BlittableJsonReaderObject sample)
        {
            _conventions = conventions;
            _sample = sample;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/ai/gen-ai/to-json-schema";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _sample).ConfigureAwait(false), _conventions)
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = response;
        }
    }


}
