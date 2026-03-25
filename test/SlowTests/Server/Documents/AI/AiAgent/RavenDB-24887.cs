using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_24887(ITestOutputHelper output) : RavenTestBase(output)
{
    public record Reply
    {
        public string Message { get; set; }
    }
    private record AddToCartArgs(string ProductId, int Quantity);
    public record Product(string Id, string Name, float Price, int QuantityInStock);
    public record Order(string Customer, Product[] Products, string Status);

    public static Product[] Products =
    [
        new Product("P1", "Laptop", 999.99f, 10),
        new Product("P2", "Smartphone", 499.99f, 25),
        new Product("P3", "Headphones", 79.99f, 50),
        new Product("P4", "Mouse", 29.99f, 100),
        new Product("P5", "Keyboard", 59.99f, 30),
        new Product("P6", "Monitor", 199.99f, 15),
        new Product("P7", "Tablet", 299.99f, 20),
        new Product("P8", "Speaker", 49.99f, 40),
        new Product("P9", "Laptop Charger", 89.99f, 35),
        new Product("P10", "Phone Charger", 19.99f, 60)
    ];

    public static Order[] Orders =
    [
        new Order("customers/1-A", [Products[0], Products[3], Products[4]], "Pending"),
        new Order("customers/1-A", [Products[2], Products[4], Products[5]], "Shipped"),
        new Order("customers/1-A", [Products[6], Products[8]], "Delivered")
    ];


    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanCallOneAgentFromAnother(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using (var session = store.OpenAsyncSession())
        {
            foreach (var p in Products)
            {
                await session.StoreAsync(p);
            }
            foreach (var o in Orders)
            {
                await session.StoreAsync(o);
            }
            await session.SaveChangesAsync();
        }

        var ordersAgentId = (await store.AI.CreateAgentAsync(
            new AiAgentConfiguration("Orders Agent", config.ConnectionStringName,
            "You are an ordering agent for an e-commerce website, help the customer as much as you can")
            {
                Queries = [new AiAgentToolQuery
                {
                    Name = "GetRecentOrders",
                    Description = "Gets recent orders for the current user",
                    ParametersSampleObject = "{}",
                    Query = "from Orders where Customer = $customerId"
                }],
                Parameters = [new AiAgentParameter("customerId", "The id of the current customer")]
            },
            new Reply()
            {
                Message = "Reply Message"
            })).Identifier;
        var productsAgentId = (await store.AI.CreateAgentAsync(
            new AiAgentConfiguration("Products Agent", config.ConnectionStringName,
                "You are an product search agent for an e-commerce website, help the customer find the right products for them")
            {
                Queries = [new AiAgentToolQuery
                {
                    Name = "SearchProducts",
                    Description = "Perform a semantic search for products",
                    ParametersSampleObject = "{\"query\": [\"The terms to look for to find the right product\"]}",
                    Query = "from Products where vector.search(embedding.text(Name), $query)"
                }],
            },
            new Reply()
            {
                Message = "Reply Message"
            })).Identifier;



        var dinnerIdentifier = (await store.AI.CreateAgentAsync(
            new AiAgentConfiguration("Shop Agent", config.ConnectionStringName,
                "You are a shop agent for an e-commerce store. Under you there are multiple additional agents that you can invoke." +
                " Your role is to make the customer happy.")
            {
                Parameters = [new AiAgentParameter("customerId", "The id of the current customer")],
                SubAgents = [new AiAgentToolSubAgent
                {
                    Identifier = ordersAgentId,
                    Description = "Use to ask everything about orders. From recent orders to order history. This is an agent, which you can communicate with using natural language"
                },new AiAgentToolSubAgent
                {
                    Identifier = productsAgentId,
                    Description = "Use to ask everything about our product catalog and inventory. This is an agent, which you can communicate with using natural language"
                },]
            },
            new Reply
            {
                Message = "Reply Message"
            })).Identifier;


        var chat = store.AI.Conversation(dinnerIdentifier, "chats/",
            new AiConversationCreationOptions().AddParameter("customerId", "customers/1-A"));

        chat.SetUserPrompt("I forgot to add a charger to my order, do you have an appropriate one in stock?");
        var result = await chat.RunAsync<Reply>();
        Assert.Contains("charger", result.Answer.Message.ToLower());
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task AnActionFromSubAgentCanBeHandledByParentConversation(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using (var session = store.OpenAsyncSession())
        {
            foreach (var p in Products)
            {
                await session.StoreAsync(p);
            }
            foreach (var o in Orders)
            {
                await session.StoreAsync(o);
            }
            await session.SaveChangesAsync();
        }

        var ordersAgentId = (await store.AI.CreateAgentAsync(
            new AiAgentConfiguration("Orders Agent", config.ConnectionStringName,
            "You are an ordering agent for an e-commerce website, help the customer as much as you can. " +
            "Make sure to return to the user any order / product IDs.")
            {
                Queries = [new AiAgentToolQuery
                {
                    Name = "GetRecentOrders",
                    Description = "Gets recent orders for the current user",
                    ParametersSampleObject = "{}",
                    Query = "from Orders where Customer = $customerId"
                }],
                Actions = [new AiAgentToolAction
                {
                    Name = "AddToCart",
                    Description = "Add an item to the cart",
                    ParametersSampleObject = JsonConvert.SerializeObject(new AddToCartArgs("The id of the product to add to the cart",1))
                }],
                Parameters = [new AiAgentParameter("customerId", "The id of the current customer")]
            },
            new Reply()
            {
                Message = "Reply Message"
            })).Identifier;
        var productsAgentId = (await store.AI.CreateAgentAsync(
            new AiAgentConfiguration("Products Agent", config.ConnectionStringName,
                "You are an product search agent for an e-commerce website, help the customer find the right products for them." +
                " Product information should always contain the product ID as well as textual data.")
            {
                Queries = [new AiAgentToolQuery
                {
                    Name = "SearchProducts",
                    Description = "Perform a semantic search for products",
                    ParametersSampleObject = "{\"query\": [\"The terms to look for to find the right product\"]}",
                    Query = "from Products where vector.search(embedding.text(Name), $query)"
                }],
            },
            new Reply()
            {
                Message = "Reply Message"
            })).Identifier;



        var dinnerIdentifier = (await store.AI.CreateAgentAsync(
            new AiAgentConfiguration("Shop Agent", config.ConnectionStringName,
                "You are a shop agent for an e-commerce store." +
                " Under you there are multiple additional agents that you can invoke, make sure to retain all identifiers in responses." +
                " Your role is to make the customer happy.")
            {
                Parameters = [new AiAgentParameter("customerId", "The id of the current customer")],
                SubAgents = [new AiAgentToolSubAgent
                {
                    Identifier = ordersAgentId,
                    Description = "Use to ask everything about orders. From recent orders to order history. Adding an item to the cart. This is an agent, which you can communicate with using natural language"
                },new AiAgentToolSubAgent
                {
                    Identifier = productsAgentId,
                    Description = "Use to ask everything about our product catalog and inventory. This is an agent, which you can communicate with using natural language"
                },]
            },
            new Reply
            {
                Message = "Reply Message"
            })).Identifier;


        var chat = store.AI.Conversation(dinnerIdentifier, "chats/",
            new AiConversationCreationOptions().AddParameter("customerId", "customers/1-A"));

        AddToCartArgs addToCartArgs = null;
        chat.Handle<AddToCartArgs>("orders-agent/AddToCart", args =>
        {
            addToCartArgs = args;
            return "Added to cart";
        });

        chat.SetUserPrompt("Do you have any laptop chargers in stock? If yes, select a suitable default option without asking questions.");
        var result = await chat.RunAsync<Reply>();
        Assert.Equal(AiConversationResult.Done, result.Status);
        chat.SetUserPrompt("Add that to my cart.");
        result = await chat.RunAsync<Reply>();
        // here we verify that we were properly called from the sub-agent 
        Assert.NotNull(addToCartArgs);
        // and were able to "solve" it properly
        Assert.Equal(AiConversationResult.Done, result.Status);
        Assert.Contains("added", result.Answer.Message);
    }
}
