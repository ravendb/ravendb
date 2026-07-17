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
}
