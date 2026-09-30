using FastTests;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ActionFixtures;

namespace QuillTests;

public class AgentConfigValidatorTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void EnforceLimit_preserves_a_skip_take_limit_within_the_cap()
    {
        Assert.Equal("from Orders limit 10, 5", AgentConfigValidator.EnforceLimit("from Orders limit 10, 5"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void EnforceLimit_caps_only_the_take_of_a_skip_take_limit()
    {
        Assert.Equal("from Orders limit 10, 32", AgentConfigValidator.EnforceLimit("from Orders limit 10, 100"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void EnforceLimit_leaves_a_parameterized_limit_untouched()
    {
        Assert.Equal("from Orders limit 32", AgentConfigValidator.EnforceLimit("from Orders limit $take"));
        Assert.Equal("from Orders limit $skip, 32", AgentConfigValidator.EnforceLimit("from Orders limit $skip, $take"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void EnforceLimit_appends_when_absent()
    {
        Assert.Equal("from Orders limit 32", AgentConfigValidator.EnforceLimit("from Orders"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void EnforceLimit_keeps_a_limit_within_the_cap()
    {
        Assert.Equal("from Orders limit 5", AgentConfigValidator.EnforceLimit("from Orders limit 5"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void EnforceLimit_caps_a_limit_over_the_cap()
    {
        Assert.Equal("from Orders limit 32", AgentConfigValidator.EnforceLimit("from Orders limit 500"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void EnforceLimit_ignores_limit_inside_a_string_literal()
    {
        Assert.Equal(
            "from Orders where Note = 'no limit 999' limit 32",
            AgentConfigValidator.EnforceLimit("from Orders where Note = 'no limit 999'"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void EnforceLimit_ignores_limit_inside_a_double_quoted_literal()
    {
        Assert.Equal(
            "from Orders where Note = \"no limit 999\" limit 32",
            AgentConfigValidator.EnforceLimit("from Orders where Note = \"no limit 999\""));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void EnforceLimit_ignores_limit_inside_a_literal_with_escaped_quote()
    {
        Assert.Equal(
            "from Orders where Note = 'it''s over the limit 999' limit 32",
            AgentConfigValidator.EnforceLimit("from Orders where Note = 'it''s over the limit 999'"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void EnforceLimit_caps_a_real_limit_after_a_literal_containing_limit()
    {
        Assert.Equal(
            "from Orders where Note = 'limit 999' limit 32",
            AgentConfigValidator.EnforceLimit("from Orders where Note = 'limit 999' limit 500"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ValidateActions_accepts_webhook_bindings_matched_without_case()
    {
        var config = ConfigWith(("create_ticket", "files a ticket"), ("archive", "archives it"));

        Assert.True(AgentConfigValidator.TryValidateActions(config, new()
        {
            ["CREATE_TICKET"] = Webhook("https://hooks.example/t"),   // key casing is irrelevant
            ["archive"] = Webhook("https://hooks.example/a"),
        }, out var errors));
        Assert.Empty(errors);

        Assert.True(AgentConfigValidator.TryValidateActions(ConfigWith(), null, out var noActions));
        Assert.Empty(noActions);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ValidateActions_reports_every_problem_it_finds()
    {
        var config = ConfigWith(("create_ticket", "files a ticket"), ("archive", "archives it"));

        Assert.False(AgentConfigValidator.TryValidateActions(config, new()
        {
            ["create_ticket"] = Webhook("not-a-url"),
            ["ghost"] = Webhook("https://hooks.example/g"),
        }, out var errors));

        Assert.Equal(
            [
                "action 'create_ticket': url must be http(s)",
                "action 'archive' has no binding",
                "binding 'ghost' has no matching action",
            ],
            errors);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ValidateActions_requires_a_name(string? name)
    {
        var config = ConfigWith((name!, "does something"));

        // a null key cannot go into a dictionary, so the null case arrives with no bindings at all —
        // it must still report rather than throw
        var bindings = name is null
            ? null
            : new Dictionary<string, WebhookBinding> { [name] = Webhook("https://h/x") };

        Assert.Contains("action name is required", ErrorsOf(config, bindings));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ValidateActions_requires_a_description()
    {
        var config = ConfigWith(("create_ticket", ""));

        Assert.Contains("action description is required",
            ErrorsOf(config, new() { ["create_ticket"] = Webhook("https://h/x") }));
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not-a-url")]
    [InlineData("/relative/only")]
    [InlineData("ftp://files.example/drop")]
    public void ValidateActions_requires_an_absolute_http_url(string? url)
    {
        var config = ConfigWith(("create_ticket", "files a ticket"));

        Assert.Contains("action 'create_ticket': url must be http(s)",
            ErrorsOf(config, new() { ["create_ticket"] = Webhook(url) }));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ValidateActions_requires_a_parameter_schema_or_sample_and_either_satisfies_it()
    {
        var config = ConfigWith(("create_ticket", "files a ticket"));
        var bindings = new Dictionary<string, WebhookBinding> { ["create_ticket"] = Webhook("https://h/x") };

        config.Actions[0].ParametersSampleObject = null;
        Assert.Contains("action 'create_ticket': parametersSampleObject or parametersSchema is required",
            ErrorsOf(config, bindings));

        config.Actions[0].ParametersSchema = """{"type":"object"}""";
        Assert.True(AgentConfigValidator.TryValidateActions(config, bindings, out var errors));
        Assert.Empty(errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ValidateActions_rejects_a_schema_and_a_sample_object_together()
    {
        var config = ConfigWith(("create_ticket", "files a ticket"));
        config.Actions[0].ParametersSchema = """{"type":"object"}""";

        // the sample object is already set, so both are: the server would silently ignore one of them
        Assert.Contains("action 'create_ticket': set parametersSampleObject or parametersSchema, not both",
            ErrorsOf(config, new() { ["create_ticket"] = Webhook("https://h/x") }));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ValidateActions_reports_bindings_that_collide_once_matched_without_case()
    {
        var config = ConfigWith(("create_ticket", "files a ticket"));

        // a case-sensitive JSON object carries both keys happily; matching them the way action names
        // are matched everywhere else cannot, and that has to read as a 400 rather than throw
        var bindings = new Dictionary<string, WebhookBinding>
        {
            ["create_ticket"] = Webhook("https://h/x"),
            ["CREATE_TICKET"] = Webhook("https://h/y"),
        };

        Assert.Contains(ErrorsOf(config, bindings), e => e.Contains("is declared more than once"));
    }

    private static AiAgentConfiguration ConfigWith(params (string Name, string Description)[] actions) => new()
    {
        Identifier = "support",
        Name = "Support",
        SystemPrompt = "You help.",
        ConnectionStringName = "demo-llm",
        Actions = actions
            .Select(a => new AiAgentToolAction
            {
                Name = a.Name,
                Description = a.Description,
                ParametersSampleObject = "{}",
            })
            .ToList(),
    };

    private static List<string> ErrorsOf(
        AiAgentConfiguration config, Dictionary<string, WebhookBinding>? bindings)
    {
        Assert.False(AgentConfigValidator.TryValidateActions(config, bindings, out var errors));
        return errors;
    }
}
