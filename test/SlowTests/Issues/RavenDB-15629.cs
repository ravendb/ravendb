using System.Net.Http;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_15629 : RavenTestBase
{
    public RavenDB_15629(ITestOutputHelper output) : base(output)
    {
    }
    
    private class GetIndexFieldsForStudioCommand : RavenCommand<object>
    {
        public GetIndexFieldsForStudioCommand()
        {
            
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/studio/index-fields";

            var payload = new { Map = @"timeSeries.map('Users', 'HeartRate', function (ts) {
                            return ts.Entries.map(entry => ({
                                HeartBeat: entry.Value,
                                Date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),
                                User: ts.DocumentId,
                                Count: 1
                            }));
                        })" };
            
            var payloadJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(payload, ctx);

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await ctx.WriteAsync(stream, payloadJson);
                }, DocumentConventions.Default)
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();
            
            Result = response;
        }

        public override bool IsReadRequest => true;
     }

    [Fact]
    public void CheckIfCorrectIndexFieldsAreReturnedForJsIndex()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var cmd = new GetIndexFieldsForStudioCommand();
                commands.Execute(cmd);

                var res = cmd.Result;
                var result = ((BlittableJsonReaderObject)res)["Results"] as BlittableJsonReaderArray;
                Assert.NotNull(result);
                Assert.Equal("HeartBeat", result[0]);
                Assert.Equal("Date", result[1]);
                Assert.Equal("User", result[2]);
                Assert.Equal("Count", result[3]);
            }
        }
    }
}
