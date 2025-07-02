using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.AI.GenAi;
using Raven.Server.Logging;
using Sparrow.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests
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
                    Name = "test-connection",
                    OllamaSettings = new OllamaSettings
                    {
                        Uri = "http://localhost:11434",
                        Model = "test-model",
                        Think = thinkValue
                    }
                }
            };

            // Create context pool for the client
            using var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(), new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()));
            
            // Create Ollama client and test WriteCustomParameters directly
            using var client = new OllamaChatCompletionClient(genAiConfig, "{}", contextPool, Raven.Client.Documents.Conventions.DocumentConventions.DefaultForServer);
            
            // Test the WriteCustomParameters method by capturing what it writes
            string capturedParameters = null;
            
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var stream = new MemoryStream())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
            {
                // Simulate calling WriteCustomParameters like the real payload generation does
                var method = typeof(OllamaChatCompletionClient).GetMethod("WriteCustomParameters", 
                    BindingFlags.NonPublic | BindingFlags.Instance);
                
                method?.Invoke(client, [writer]);
                await writer.FlushAsync();
                
                capturedParameters = Encoding.UTF8.GetString(stream.ToArray());
            }

            // Assert - Verify the parameters contain or don't contain the think parameter
            if (expectedJsonContent == null)
            {
                // When Think is null, no parameters should be written
                Assert.True(string.IsNullOrEmpty(capturedParameters));
            }
            else
            {
                // When Think has a value, verify the exact JSON content is written
                Assert.Contains(expectedJsonContent, capturedParameters);
            }
        }
    }
}
