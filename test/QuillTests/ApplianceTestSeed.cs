using System.Net.Http.Json;
using Xunit;

namespace QuillTests;

/// <summary>
/// Seeds a mock connection string + agent in an app's per-app DB so the channel / embed / setup-try
/// endpoints (which resolve the agent from the database, not a compile-time registry) have a real agent to
/// bind to. This only persists the CS + agent; it does not dial the provider — but a test that then runs a
/// turn (setup/try, embed chat) will try to contact the Ollama endpoint and surface an error frame, which is
/// expected. Idempotent: the CS and agent upsert.
/// </summary>
internal static class ApplianceTestSeed
{
    public static async Task SeedMockAgentAsync(HttpClient client, string slug = "my-app", string agentId = "demo-agent")
    {
        var csResp = await client.PostAsJsonAsync(
            $"/api/apps/{slug}/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode,
            $"seed connection-string returned {csResp.StatusCode}: {await csResp.Content.ReadAsStringAsync()}");

        var agentResp = await client.PostAsJsonAsync(
            $"/api/apps/{slug}/setup/agent",
            new
            {
                identifier = agentId,
                name = "Demo Agent",
                systemPrompt = "You are a placeholder demo agent.",
                connectionStringName = "demo-llm",
            });
        Assert.True(agentResp.IsSuccessStatusCode,
            $"seed agent returned {agentResp.StatusCode}: {await agentResp.Content.ReadAsStringAsync()}");
    }
}
