using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_25142 : RavenTestBase
    {
        public RavenDB_25142(ITestOutputHelper output) : base(output)
        {
        }

        private class AgentResponse
        {
            public string Request { get; set; } = "";
            public string Response { get; set; } = "";
            public string CustomerId { get; set; } = "";
            public string[] RelatedProducts { get; set; } = [""];
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Can_recreate_negative_total_tokens_with_truncation(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentConfig = new AiAgentConfiguration("agent-007", config.ConnectionStringName, "you are agent used to help with finding orders")
            {
                SampleObject = JsonConvert.SerializeObject(new AgentResponse())
            };
            await store.AI.CreateAgentAsync(agentConfig);

            var conversation = store.AI.Conversation("agent-007", "chats/", new AiConversationCreationOptions());

            var turnUsages = new List<AiUsage>();

            conversation.AddUserPrompt(["Hi", "Hello", "HEHE"]);

            turnUsages.Add((await conversation.RunAsync<object>()).Usage);


            conversation.AddUserPrompt(["What would you recommend with cheese?", "What are some great cheese and beer pairings to try, like how a sharp cheddar goes well with an IPA or a creamy brie enhances a stout?"]);

            turnUsages.Add((await conversation.RunAsync<object>()).Usage);

            conversation.AddUserPrompt(["What would you recommend with meat?", "What are some great chips and fish pairings to try, like how a sharp cheddar goes well with an IPA or a creamy brie enhances a stout?"]);
            conversation.AddUserPrompt(["Answer: HI"]);
            turnUsages.Add((await conversation.RunAsync<object>()).Usage);
            
            using (var session = store.OpenAsyncSession())
            {
                var convDoc = await session.LoadAsync<BlittableJsonReaderObject>(conversation.Id);

                convDoc.TryGet("Messages", out BlittableJsonReaderArray messages);
                Assert.NotNull(messages);

                var lastMessage = messages.Last() as BlittableJsonReaderObject;
                Assert.NotNull(lastMessage);

                lastMessage.TryGet("usage", out BlittableJsonReaderObject usageJson);
                Assert.NotNull(usageJson);

                usageJson.TryGet("TotalTokens", out long totalTokens);
                Assert.True(totalTokens > 0,
                    $"BUG REPRODUCED: Expected TotalTokens to be positive, and it was {totalTokens}.{Environment.NewLine}{DumpDiagnostics(convDoc, conversation.Id, turnUsages)}");
                usageJson.TryGet("PromptTokens", out long promptTokens);
                Assert.True(promptTokens > 0,
                    $"BUG REPRODUCED: Expected PromptTokens to be positive, and it was {promptTokens}.{Environment.NewLine}{DumpDiagnostics(convDoc, conversation.Id, turnUsages)}");
            }
        }

        private static string DumpDiagnostics(BlittableJsonReaderObject convDoc, string conversationId, List<AiUsage> turnUsages)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"ConversationId: {conversationId}");
            sb.AppendLine($"convDoc != null: {convDoc != null}");

            sb.AppendLine($"Per-turn usage as returned by RunAsync ({turnUsages?.Count ?? 0} turns):");
            for (var i = 0; i < (turnUsages?.Count ?? 0); i++)
            {
                var u = turnUsages[i];
                sb.AppendLine(u == null
                    ? $"  turn[{i}]: <null>"
                    : $"  turn[{i}]: PromptTokens={u.PromptTokens}, CompletionTokens={u.CompletionTokens}, TotalTokens={u.TotalTokens}, CachedTokens={u.CachedTokens}, ReasoningTokens={u.ReasoningTokens}");
            }

            if (convDoc == null)
                return sb.ToString();

            sb.AppendLine($"convDoc.Size: {convDoc.Size}");
            sb.AppendLine($"convDoc property names: {string.Join(",", convDoc.GetPropertyNames())}");

            var cur = TryGetObj(convDoc, "CurrentUsage");
            sb.AppendLine($"convDoc.CurrentUsage: {cur?.ToString() ?? "<missing>"}");
            sb.AppendLine($"convDoc.TotalUsage: {TryGetObj(convDoc, "TotalUsage")?.ToString() ?? "<missing>"}");

            var messages = TryGetArr(convDoc, "Messages");
            sb.AppendLine($"messages != null: {messages != null}, messages.Length: {messages?.Length.ToString() ?? "<null>"}");
            if (messages == null)
                return sb.ToString();

            var withUsage = Enumerable.Range(0, messages.Length)
                .Select(i => messages[i] as BlittableJsonReaderObject)
                .Where(m => TryGetObj(m, "usage") != null)
                .ToList();
            var lastUsage = TryGetObj(withUsage.LastOrDefault(), "usage");
            var prevUsage = withUsage.Count > 1 ? TryGetObj(withUsage[withUsage.Count - 2], "usage") : null;
            sb.AppendLine($"last msg with usage: {FormatUsage(lastUsage)}");
            sb.AppendLine($"prev msg with usage: {FormatUsage(prevUsage)}");

            // Implied "previous" used by GetUsageDifference for the last message = doc.CurrentUsage - lastMessage.usage.
            // If any component goes negative, Math.Max in GetUsageDifference clamped the diff to 0 -> the bug.
            if (cur != null && lastUsage != null)
            {
                long Diff(string name) => GetLong(cur, name) - GetLong(lastUsage, name);
                var clamped = GetLong(lastUsage, "TotalTokens") == 0 || GetLong(lastUsage, "PromptTokens") == 0;
                sb.AppendLine($"implied previous for last msg (doc.CurrentUsage - lastMessage.usage): PromptTokens={Diff("PromptTokens")}, CompletionTokens={Diff("CompletionTokens")}, TotalTokens={Diff("TotalTokens")}, ReasoningTokens={Diff("ReasoningTokens")}, wasClampedInGetUsageDifference={clamped}");
            }

            for (var i = 0; i < messages.Length; i++)
            {
                var msg = messages[i] as BlittableJsonReaderObject;
                if (msg == null)
                {
                    sb.AppendLine($"  msg[{i}]: <not a BlittableJsonReaderObject>");
                    continue;
                }
                var role = msg.TryGet("role", out string r) ? r : "<missing>";
                sb.AppendLine($"  msg[{i}]: role={role}, size={msg.Size}");
                var usage = TryGetObj(msg, "usage");
                sb.AppendLine($"    usage: {(usage != null ? FormatUsage(usage) : "<missing>")}");
                sb.AppendLine($"    body: {msg}");
            }

            return sb.ToString();
        }

        private static BlittableJsonReaderObject TryGetObj(BlittableJsonReaderObject obj, string name)
        {
            try { return obj != null && obj.TryGet(name, out BlittableJsonReaderObject v) ? v : null; }
            catch { return null; }
        }

        private static BlittableJsonReaderArray TryGetArr(BlittableJsonReaderObject obj, string name)
        {
            try { return obj != null && obj.TryGet(name, out BlittableJsonReaderArray v) ? v : null; }
            catch { return null; }
        }

        private static long GetLong(BlittableJsonReaderObject obj, string name)
        {
            try { return obj != null && obj.TryGet(name, out long v) ? v : 0; }
            catch { return 0; }
        }

        private static string FormatUsage(BlittableJsonReaderObject usage)
        {
            if (usage == null)
                return "<null>";
            return $"PromptTokens={GetLong(usage, "PromptTokens")}, CompletionTokens={GetLong(usage, "CompletionTokens")}, TotalTokens={GetLong(usage, "TotalTokens")}, CachedTokens={GetLong(usage, "CachedTokens")}, ReasoningTokens={GetLong(usage, "ReasoningTokens")}";
        }
    }
}

