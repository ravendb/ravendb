using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class WizardAgentEndpointsTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_returns_404_for_unknown_slug()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.ProvisionAgentAsync("nonexistent", new AiAgentConfiguration
        {
            Name = "Support Bot", SystemPrompt = "You help.", ConnectionStringName = "demo-llm",
        }));

        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_returns_agentId_and_references_operator_provided_cs()
    {
        await using var app = await NewAppAsync();

        var provisioned = await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Name = "Support Bot",
            SystemPrompt = "You are a helpful support agent for the Northwind store.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });
        Assert.False(string.IsNullOrEmpty(provisioned.AgentId), "agentId was empty");

        var agents = await app.Store.Maintenance.ForDatabase(app.Slug).SendAsync(new GetAiAgentsOperation());
        var agent = Assert.Single(agents.AiAgents);
        Assert.Equal(Host.ConnectionStringName, agent.ConnectionStringName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_accepts_explicit_null_parameters()
    {
        await using var app = await NewAppAsync();

        // explicit null Parameters binds cleanly
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Name = "Support Bot", SystemPrompt = "You help.", ConnectionStringName = app.Host.ConnectionStringName, Parameters = null,
        });

        var agents = await app.Store.Maintenance.ForDatabase(app.Slug).SendAsync(new GetAiAgentsOperation());
        Assert.Single(agents.AiAgents);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_returns_400_for_missing_connection_string_name()
    {
        await using var app = await NewAppAsync();

        // leave ConnectionStringName unset (null)
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Name = "Support Bot", SystemPrompt = "You help.",
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_returns_400_for_unknown_connection_string_name()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Name = "Support Bot", SystemPrompt = "You help.", ConnectionStringName = "ghost-llm",
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_returns_400_when_server_rejects_configuration()
    {
        await using var app = await NewAppAsync();

        // space in tool-query name passes intake but the server rejects it — 400, not a bubbled 500
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Name = "Support Bot",
            SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Queries = [new AiAgentToolQuery { Name = "bad name", Description = "lookup", Query = "from Orders" }],
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_persists_request_name_prompt_and_queries()
    {
        await using var app = await NewAppAsync();

        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support-bot",
            Name = "Support Bot",
            SystemPrompt = "You are a Northwind support agent. Answer using the orders data.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "findOrdersByCustomer",
                    Description = "Find orders for a given customer id.",
                    Query = "from Orders where Customer = $customerId",
                    ParametersSampleObject = """{"customerId":"ALFKI"}""",
                },
            ],
        });

        var agents = await app.Store.Maintenance.ForDatabase(app.Slug).SendAsync(new GetAiAgentsOperation());
        var agent = Assert.Single(agents.AiAgents);
        Assert.Equal("support-bot", agent.Identifier);
        Assert.Equal("Support Bot", agent.Name);
        Assert.Equal("You are a Northwind support agent. Answer using the orders data.", agent.SystemPrompt);
        var query = Assert.Single(agent.Queries);
        Assert.Equal("findOrdersByCustomer", query.Name);
        Assert.Equal("from Orders where Customer = $customerId limit 32", query.Query);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_returns_400_for_missing_name()
    {
        await using var app = await NewAppAsync();

        // leave Name unset (null)
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            SystemPrompt = "You help.", ConnectionStringName = app.Host.ConnectionStringName,
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_returns_400_for_missing_system_prompt()
    {
        await using var app = await NewAppAsync();

        // leave SystemPrompt unset (null)
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Name = "Support Bot", ConnectionStringName = app.Host.ConnectionStringName,
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_creates_agent_when_sample_object_omitted()
    {
        // endpoint defaults SampleObject when it and OutputSchema are both omitted
        await using var app = await NewAppAsync();

        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Name = "Support Bot", SystemPrompt = "You help.", ConnectionStringName = app.Host.ConnectionStringName,
        });

        var agents = await app.Store.Maintenance.ForDatabase(app.Slug).SendAsync(new GetAiAgentsOperation());
        var agent = Assert.Single(agents.AiAgents);

        using var sample = JsonDocument.Parse(agent.SampleObject);
        Assert.True(sample.RootElement.TryGetProperty("reply", out var reply));
        Assert.Equal("", reply.GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_rejects_actions()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Name = "Support Bot",
            SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Actions = [new AiAgentToolAction { Name = "sendEmail", Description = "send an email" }],
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_endpoint_rejects_sub_agents()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Name = "Support Bot",
            SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
            SubAgents = [new AiAgentToolSubAgent { Identifier = "helper", Description = "assist" }],
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("demo-agent")]   // valid agentId
    [InlineData("ghost-agent")]  // invalid agentId — must not leak existence via 400 vs 404
    public async Task Channel_endpoint_returns_404_for_unknown_slug(string agentId)
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.ProvisionChannelAsync("nonexistent", 
            new ProvisionChannelRequest(ChannelType.IFrame, agentId, new[] { "http://localhost" })));

        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_endpoint_returns_channelId_for_known_app()
    {
        await using var app = await NewAppAsync();
        await SeedDemoAgentAsync(app);

        var provisioned = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { "http://localhost" }));
        var channelId = provisioned.ChannelId;
        Assert.False(string.IsNullOrEmpty(channelId), "channelId was empty");
        // Guid "N": 32 hex chars, 128 bits of entropy, no prefix
        Assert.Equal(32, channelId.Length);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_endpoint_returns_501_for_recognized_but_unimplemented_type()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsApp, "demo-agent", Array.Empty<string>())));

        Assert.Equal(HttpStatusCode.NotImplemented, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_endpoint_rejects_unknown_type()
    {
        await using var app = await NewAppAsync();

        // raw: unknown channel-type string can't be expressed as the ChannelType enum
        var resp = await Host.Client.PostAsJsonAsync(QuillRoutes.SetupChannel(app.Slug),
            new { type = "carrier-pigeon", agentId = "demo-agent", allowedOrigins = Array.Empty<string>() });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_endpoint_returns_400_when_allowedOrigins_missing()
    {
        await using var app = await NewAppAsync();
        await SeedDemoAgentAsync(app);

        // pass AllowedOrigins null
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", null)));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_endpoint_returns_400_when_type_missing()
    {
        await using var app = await NewAppAsync();

        // pass Type null
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(null, "demo-agent", Array.Empty<string>())));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_repeated_provision_creates_distinct_channels()
    {
        await using var app = await NewAppAsync();
        await SeedDemoAgentAsync(app);

        var channelId1 = (await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { "http://site-a.example" }, "Site A"))).ChannelId;

        var channelId2 = (await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { "http://site-b.example" }, "Site B"))).ChannelId;

        Assert.NotEqual(channelId1, channelId2);

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var channels = await session.Advanced.LoadStartingWithAsync<Channel>("channels/");
        Assert.Equal(2, channels.Count());

        var channelA = await session.LoadAsync<Channel>($"channels/{channelId1}");
        Assert.Equal("Site A", channelA.DisplayName);
        Assert.Equal(new[] { "http://site-a.example" }, channelA.AllowedOrigins);

        var channelB = await session.LoadAsync<Channel>($"channels/{channelId2}");
        Assert.Equal("Site B", channelB.DisplayName);
        Assert.Equal(new[] { "http://site-b.example" }, channelB.AllowedOrigins);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("https://example.com/", "https://example.com")]
    [InlineData("http://example.com:8080/", "http://example.com:8080")]
    [InlineData("https://example.com", "https://example.com")]
    public async Task Channel_endpoint_normalizes_origin_on_persist(string supplied, string expected)
    {
        await using var app = await NewAppAsync();
        await SeedDemoAgentAsync(app);

        await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { supplied }));

        await app.WaitForIndexingAsync();

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var channel = await session.Query<Channel>().FirstAsync();
        Assert.Single(channel.AllowedOrigins);
        Assert.Equal(expected, channel.AllowedOrigins[0]);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("http://example.com/app")]
    [InlineData("https://example.com/foo/bar")]
    [InlineData("https://example.com/?q=1")]
    [InlineData("https://example.com/#frag")]
    public async Task Channel_endpoint_rejects_origin_with_path_query_or_fragment(string badOrigin)
    {
        await using var app = await NewAppAsync();
        await SeedDemoAgentAsync(app);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { badOrigin })));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("*")]
    [InlineData("example.com")]
    [InlineData("ftp://example.com")]
    [InlineData("")]
    [InlineData("https://user:pass@example.com")]
    public async Task Channel_endpoint_rejects_invalid_allowed_origin(string badOrigin)
    {
        await using var app = await NewAppAsync();
        await SeedDemoAgentAsync(app);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { badOrigin })));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_endpoint_rejects_too_many_allowed_origins()
    {
        await using var app = await NewAppAsync();
        await SeedDemoAgentAsync(app);

        var tooMany = Enumerable.Range(0, 33).Select(i => $"http://example{i}.com").ToArray();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", tooMany)));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("ab")]
    [InlineData("name\twith\ttabs")]
    public async Task Channel_endpoint_rejects_invalid_display_name(string badName)
    {
        await using var app = await NewAppAsync();
        await SeedDemoAgentAsync(app);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { "http://localhost" }, badName)));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_endpoint_rejects_too_long_display_name()
    {
        await using var app = await NewAppAsync();
        await SeedDemoAgentAsync(app);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { "http://localhost" }, new string('x', 201))));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_doc_lands_in_Channels_collection()
    {
        await using var app = await NewAppAsync();
        await SeedDemoAgentAsync(app);

        await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { "http://localhost" }));

        await app.WaitForIndexingAsync();

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var ch = await session.Query<Channel>().FirstAsync();
        var collection = session.Advanced.GetMetadataFor(ch)["@collection"]!.ToString();
        Assert.Equal("@channels", collection);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_endpoint_persists_canonical_agent_id_casing()
    {
        await using var app = await NewAppAsync();
        await SeedDemoAgentAsync(app);

        await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "Demo-Agent", new[] { "http://localhost" }));

        await app.WaitForIndexingAsync();

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var channel = await session.Query<Channel>().FirstAsync();
        Assert.Equal("demo-agent", channel.AgentId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_binds_to_an_operator_defined_non_demo_agent()
    {
        await using var app = await NewAppAsync();

        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "order-support",
            Name = "Order Support",
            SystemPrompt = "You are the Northwind order-support agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "findOrdersByCustomer",
                    Description = "Find orders for a given customer id.",
                    Query = "from Orders where Customer = $customerId",
                    ParametersSampleObject = """{"customerId":"ALFKI"}""",
                },
            ],
        });

        await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "order-support", new[] { "http://localhost" }));

        await app.WaitForIndexingAsync();

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var channel = await session.Query<Channel>().FirstAsync();
        Assert.Equal("order-support", channel.AgentId);

        var agents = await app.Store.Maintenance.ForDatabase(app.Slug).SendAsync(new GetAiAgentsOperation());
        var agent = Assert.Single(agents.AiAgents);
        Assert.Equal("order-support", agent.Identifier);
        Assert.Equal("You are the Northwind order-support agent.", agent.SystemPrompt);
        Assert.Equal("findOrdersByCustomer", Assert.Single(agent.Queries).Name);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_endpoint_returns_400_for_unknown_agent()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "ghost-agent", new[] { "http://localhost" })));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    private static async Task SeedDemoAgentAsync(QuillApp app)
    {
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo-agent",
            Name = "Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });
    }
}
