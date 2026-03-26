using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Server.Documents.AI
{
    public class OllamaThinkParameterTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(true, "\"think\":true")]
        [InlineData(false, "\"think\":false")]
        [InlineData(null, null)] // null means parameter should be omitted entirely
        public async Task OllamaClient_ThinkParameter_ShouldSerializeInChatCompletionPayload(bool? thinkValue, string expectedJsonContent)
        {
            // Arrange - Create Ollama configuration with think parameter
            var genAiConfig = new GenAiConfiguration
            {
                Name = "test-config",
                ConnectionStringName = "test-connection", 
                Collection = "TestDocs",
                Prompt = "Test prompt",
                Connection = new AiConnectionString
                {
                    ModelType = AiModelType.Chat,
                    Name = "test-connection",
                    OllamaSettings = new OllamaSettings
                    {
                        Uri = "http://localhost:11434",
                        Model = "test-model",
                        Think = thinkValue
                    }
                }
            };

            using var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(), new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()));
            using var client = ChatCompletionClient.CreateChatCompletionClient(contextPool, genAiConfig.Connection);
            
            string capturedParameters;
            
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var stream = new MemoryStream())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
            {
                client.WriteCompletionRequestPayload(writer, context, [], [], [], true, false, ChatCompletionClient.EmptySchema);
                await writer.FlushAsync();
                
                capturedParameters = Encoding.UTF8.GetString(stream.ToArray());
            }

            // Assert - Verify the parameters contain or don't contain the think parameter
            if (expectedJsonContent == null)
            {
                // When Think is null, no parameters should be written
                Assert.DoesNotContain("think", capturedParameters);
            }
            else
            {
                // When Think has a value, verify the exact JSON content is written
                Assert.Contains(expectedJsonContent, capturedParameters);
            }
        }
    }
}
