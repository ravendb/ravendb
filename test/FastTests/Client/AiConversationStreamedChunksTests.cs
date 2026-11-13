using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI.Agents;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class AiConversationStreamedChunksTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Ai)]
    public async Task StreamedChunksWrittenByServerAreDecodedVerbatim()
    {
        string[] chunks =
        [
            "plain",
            "quote \" and backslash \\",
            "line\nbreak\r\ttab",
            "control \u0001\u001f",
            "unicode żółć 😀",
            "</script>&'",
            ""
        ];

        var received = new List<string>();
        var operation = new RunConversationOperation<object>("agents/1", "chats/1", [], [], [], new AiConversationCreationOptions(), null, "Answer", chunk =>
        {
            received.Add(chunk);
            return Task.CompletedTask;
        });

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var stream = new MemoryStream())
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
            {
                foreach (string chunk in chunks)
                {
                    using (LazyStringValue lazy = context.GetLazyString(chunk))
                        writer.WriteString(lazy);
                    writer.WriteRawString("\r\n"u8);
                }
            }

            stream.Position = 0;

            var command = operation.GetCommand(DocumentConventions.Default, context);
            await command.SetResponseRawAsync(new HttpResponseMessage(), stream, context);
        }

        Assert.Equal(chunks, received);
    }
}
