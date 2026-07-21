using FastTests;
using Raven.Quill.Agents;
using Tests.Infrastructure;
using Xunit;

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
}
