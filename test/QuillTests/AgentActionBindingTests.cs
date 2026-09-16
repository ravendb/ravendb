using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ActionFixtures;

namespace QuillTests;

public class AgentActionBindingTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_with_actions_persists_the_sidecar()
    {
        await using var app = await NewAppAsync();

        var agentId = (await app.ProvisionAgentAsync(AgentWith(app,
            ("create_ticket", Webhook("https://hooks.example/t", secret: "s3cret")),
            ("archive", Webhook("https://hooks.example/a"))))).AgentId;

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var doc = await session.LoadAsync<AgentActionBindings>(AgentActionBindings.IdFor(agentId));

        Assert.NotNull(doc);
        Assert.Equal(["archive", "create_ticket"], doc.Bindings.Keys.Order());
        // distinct urls: each key kept its own binding, not a shared or last-write-wins one
        Assert.Equal("https://hooks.example/t", doc.Bindings["create_ticket"].Url);
        Assert.Equal("s3cret", doc.Bindings["create_ticket"].Secret);
        Assert.Equal("https://hooks.example/a", doc.Bindings["archive"].Url);
        Assert.Equal("@agent-actions", session.Advanced.GetMetadataFor(doc)["@collection"]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_replaces_the_sidecar_wholesale()
    {
        await using var app = await NewAppAsync();
        var agentId = (await app.ProvisionAgentAsync(AgentWith(app,
            ("create_ticket", Webhook("https://hooks.example/t")),
            ("archive", Webhook("https://hooks.example/a"))))).AgentId;

        await app.EditAgentAsync(AgentWith(app, ("archive", Webhook("https://hooks.example/a2"))));

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var doc = await session.LoadAsync<AgentActionBindings>(AgentActionBindings.IdFor(agentId));

        Assert.NotNull(doc);
        Assert.Equal(["archive"], doc.Bindings.Keys);
        // a2, not the provisioned url: proves the surviving entry was rewritten, not left stale
        Assert.Equal("https://hooks.example/a2", doc.Bindings["archive"].Url);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_to_zero_actions_removes_the_sidecar()
    {
        await using var app = await NewAppAsync();
        var agentId = (await app.ProvisionAgentAsync(
            AgentWith(app, ("create_ticket", Webhook("https://hooks.example/t"))))).AgentId;

        await app.EditAgentAsync(AgentWith(app));

        using var session = app.Store.OpenAsyncSession(app.Slug);
        Assert.Null(await session.LoadAsync<AgentActionBindings>(AgentActionBindings.IdFor(agentId)));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Get_returns_bindings_and_the_secret_round_trips()
    {
        await using var app = await NewAppAsync();
        var agentId = (await app.ProvisionAgentAsync(AgentWith(app,
            ("create_ticket", Webhook("https://hooks.example/t", secret: "s3cret"))))).AgentId;

        var details = await app.GetAgentAsync(agentId);
        Assert.Equal("s3cret", details.ActionBindings["create_ticket"].Secret);

        // what you GET is what you POST back: the operator edit form has no other way to keep it
        await app.EditAgentAsync(new EditAgentRequest(details.Configuration, details.ActionBindings));

        var reread = await app.GetAgentAsync(agentId);
        Assert.Equal("s3cret", reread.ActionBindings["create_ticket"].Secret);
        Assert.Equal("https://hooks.example/t", reread.ActionBindings["create_ticket"].Url);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Both_write_endpoints_reject_an_action_without_a_binding()
    {
        await using var app = await NewAppAsync();
        var bindingless = new EditAgentRequest(
            AgentWith(app, ("create_ticket", Webhook("https://h/x"))).Configuration, null);

        var onProvision = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionAgentAsync(bindingless));
        Assert.Equal(HttpStatusCode.BadRequest, onProvision.StatusCode);
        Assert.Contains("action 'create_ticket' has no binding", onProvision.Body);

        await app.ProvisionAgentAsync(AgentWith(app, ("create_ticket", Webhook("https://hooks.example/t"))));

        var onEdit = await Assert.ThrowsAsync<QuillHttpException>(() => app.EditAgentAsync(bindingless));
        Assert.Equal(HttpStatusCode.BadRequest, onEdit.StatusCode);
        Assert.Contains("action 'create_ticket' has no binding", onEdit.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Deleting_the_agent_deletes_the_sidecar()
    {
        await using var app = await NewAppAsync();
        var agentId = (await app.ProvisionAgentAsync(
            AgentWith(app, ("create_ticket", Webhook("https://hooks.example/t"))))).AgentId;

        await app.DeleteAgentAsync(agentId);

        using var session = app.Store.OpenAsyncSession(app.Slug);
        Assert.Null(await session.LoadAsync<AgentActionBindings>(AgentActionBindings.IdFor(agentId)));
    }

    internal static EditAgentRequest AgentWith(
        QuillApp app, params (string Name, WebhookBinding Binding)[] actions) => new(
        new AiAgentConfiguration
        {
            Identifier = "support",
            Name = "Support",
            SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Actions = actions
                .Select(a => new AiAgentToolAction(a.Name, $"performs {a.Name}")
                {
                    ParametersSampleObject = """{"subject":"sample"}""",
                })
                .ToList(),
        },
        actions.ToDictionary(a => a.Name, a => a.Binding));
}
