using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class AiAgentPromptCacheKey(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int ProviderLimit = 64;

    private const string LongConversationId =
        "UserChats/marcus@example.com/support/escalations/semrush-specialist/4716-A";

    // Computed outside the server (b2sum -l 256), so it pins the derivation instead of echoing it.
    private const string LongConversationIdDigest = "dhzGcDQyn9cySqfty7GrqMVOa8WEvQIAHfFx2J9maKQ=";

    [RavenFact(RavenTestCategory.Ai)]
    public async Task AnIdLongerThanTheLimitIsHashedDownToFit()
    {
        Assert.True(LongConversationId.Length > ProviderLimit,
            $"the id under test has to exceed the limit, it is {LongConversationId.Length} characters");

        using var store = GetDocumentStore();

        var key = await CaptureCacheKeyAsync(store, LongConversationId, "what is 2+2?");

        Assert.NotNull(key);
        Assert.True(key.Length <= ProviderLimit, $"prompt_cache_key was {key.Length} characters: '{key}'");
        Assert.NotEqual(LongConversationId, key);
        Assert.DoesNotContain("marcus@example.com", key);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task TheSameConversationKeepsTheSameKeyAcrossTurns()
    {
        using var store = GetDocumentStore();

        var first = await CaptureCacheKeyAsync(store, LongConversationId, "first");
        var second = await CaptureCacheKeyAsync(store, LongConversationId, "second");

        // Against the digest rather than against each other: that also catches a per-process hash.
        Assert.Equal(LongConversationIdDigest, first);
        Assert.Equal(LongConversationIdDigest, second);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task AnIdThatWouldHaveFitIsHashedAsWell()
    {
        const string shortId = "chats/1-A";
        const string shortIdDigest = "wi0taecRRRwlX/0aQF9xFvTFI1qD2IlPVgwDHI+/h0o=";

        Assert.True(shortId.Length <= ProviderLimit, "the id under test has to fit the limit as it is");

        using var store = GetDocumentStore();

        var key = await CaptureCacheKeyAsync(store, shortId, "what is 2+2?");

        Assert.Equal(shortIdDigest, key);
        Assert.NotEqual(shortId, key);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ConversationsSharingALongPrefixGetDifferentKeys()
    {
        using var store = GetDocumentStore();

        // truncating to the limit would give both of these the same key and merge their caches
        var a = await CaptureCacheKeyAsync(store, LongConversationId + "/first-conversation", "x");
        var b = await CaptureCacheKeyAsync(store, LongConversationId + "/second-conversation", "x");

        Assert.NotEqual(a, b);
    }

    private async Task<string> CaptureCacheKeyAsync(IDocumentStore store, string conversationId, string prompt)
    {
        string captured = null;

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            string changeVector;
            using (context.OpenReadTransaction())
                changeVector = database.DocumentsStorage.Get(context, conversationId)?.ChangeVector;

            var handler = new MockLlmConversationHandler(Server.ServerStore, database, onRequest: payload =>
            {
                captured = (string)payload["prompt_cache_key"];
                return null;
            })
            {
                Authentication = null
            };

            handler.Initialize(Agent(), conversationId, new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = prompt,
                OutputOptions = new AiServerOutputOptions { NoSchema = true }
            }, changeVector);

            await handler.HandleRequestAsync(context, CancellationToken.None);
        }

        return captured;
    }

    private static AiAgentConfiguration Agent() =>
        new("cache-key-agent", "fake-connection", "Answer briefly.") { Identifier = "cache-key-agent" };
}
