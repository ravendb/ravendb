using System.Collections.Generic;
using System.Reflection;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues;

public class RavenDB_25485 : RavenTestBase
{
    public RavenDB_25485(ITestOutputHelper output) : base(output)
    {
    }

    private class ProductInfo
    {
        public string Name { get; set; }
        public decimal Price { get; set; }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void AddActionResponseShouldWorkWhenReturningAList()
    {
        using var store = GetDocumentStore();

        var chat = store.AI.Conversation("agents/test", "chats/123", new AiConversationCreationOptions());

        var list = new List<ProductInfo>
        {
            new() { Name = "Widget", Price = 9.99m },
            new() { Name = "Gadget", Price = 19.99m }
        };

        chat.AddActionResponse("tool-1", list);

        var result = JArray.Parse(GetActionResponseContent(chat, "tool-1"));
        Assert.Equal(2, result.Count);
        Assert.Equal("Widget", result[0]["Name"].Value<string>());
        Assert.Equal(9.99m, result[0]["Price"].Value<decimal>());
        Assert.Equal("Gadget", result[1]["Name"].Value<string>());
        Assert.Equal(19.99m, result[1]["Price"].Value<decimal>());
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void AddActionResponseShouldWorkWhenReturningAnArray()
    {
        using var store = GetDocumentStore();

        var chat = store.AI.Conversation("agents/test", "chats/123", new AiConversationCreationOptions());

        var array = new[]
        {
            new ProductInfo { Name = "Widget", Price = 9.99m },
            new ProductInfo { Name = "Gadget", Price = 19.99m }
        };

        chat.AddActionResponse("tool-1", array);

        var result = JArray.Parse(GetActionResponseContent(chat, "tool-1"));
        Assert.Equal(2, result.Count);
        Assert.Equal("Widget", result[0]["Name"].Value<string>());
        Assert.Equal(9.99m, result[0]["Price"].Value<decimal>());
        Assert.Equal("Gadget", result[1]["Name"].Value<string>());
        Assert.Equal(19.99m, result[1]["Price"].Value<decimal>());
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void AddActionResponseShouldWorkWhenReturningAnEmptyList()
    {
        using var store = GetDocumentStore();

        var chat = store.AI.Conversation("agents/test", "chats/123", new AiConversationCreationOptions());

        chat.AddActionResponse("tool-1", new List<ProductInfo>());

        var result = JArray.Parse(GetActionResponseContent(chat, "tool-1"));
        Assert.Equal(0, result.Count);
    }

    private static string GetActionResponseContent(IAiConversationOperations chat, string toolId)
    {
        var field = chat.GetType().GetField("_actionResponses", BindingFlags.NonPublic | BindingFlags.Instance);
        var responses = (Dictionary<string, AiAgentActionResponse>)field.GetValue(chat);
        return responses[toolId].Content;
    }
}
