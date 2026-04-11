using System.Collections.Generic;
using Raven.Client.Documents.AI;
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
    public void HandleShouldWorkWhenReturningAList()
    {
        using var store = GetDocumentStore();

        var chat = store.AI.Conversation("agents/test", "chats/123", new AiConversationCreationOptions());

        var list = new List<ProductInfo>
        {
            new() { Name = "Widget", Price = 9.99m },
            new() { Name = "Gadget", Price = 19.99m }
        };

        chat.AddActionResponse("tool-1", list);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void HandleShouldWorkWhenReturningAnArray()
    {
        using var store = GetDocumentStore();

        var chat = store.AI.Conversation("agents/test", "chats/123", new AiConversationCreationOptions());

        var array = new[]
        {
            new ProductInfo { Name = "Widget", Price = 9.99m },
            new ProductInfo { Name = "Gadget", Price = 19.99m }
        };

        chat.AddActionResponse("tool-1", array);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void HandleShouldWorkWhenReturningAnEmptyList()
    {
        using var store = GetDocumentStore();

        var chat = store.AI.Conversation("agents/test", "chats/123", new AiConversationCreationOptions());

        chat.AddActionResponse("tool-1", new List<ProductInfo>());
    }
}
