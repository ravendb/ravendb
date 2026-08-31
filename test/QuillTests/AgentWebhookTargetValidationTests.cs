using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AgentWebhookTargetValidationTests
{
    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("http://127.0.0.1:8443/hook")]
    [InlineData("http://localhost/hook")]
    [InlineData("http://internal.localhost/hook")]
    [InlineData("http://0.0.0.0/hook")]
    [InlineData("http://10.0.0.5/hook")]
    [InlineData("http://100.64.0.1/hook")]
    [InlineData("http://169.254.169.254/latest/meta-data")]
    [InlineData("http://172.16.0.1/hook")]
    [InlineData("http://192.168.1.1/hook")]
    [InlineData("http://[::1]/hook")]
    [InlineData("http://[fd00::1]/hook")]
    [InlineData("http://[fe80::1]/hook")]
    [InlineData("http://[::ffff:127.0.0.1]/hook")]
    public void Private_and_loopback_targets_are_rejected_by_default(string url)
    {
        var ok = AgentConfigValidator.TryValidateActions(
            Config(), Bindings(url), allowPrivateWebhookTargets: false, out var errors);

        Assert.False(ok);
        var error = Assert.Single(errors);
        Assert.Contains("must not target", error);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("https://hooks.example.com/hook")]
    [InlineData("http://8.8.8.8/hook")]
    [InlineData("http://11.0.0.1/hook")]
    [InlineData("http://172.32.0.1/hook")]
    public void Public_targets_pass_without_the_override(string url)
    {
        var ok = AgentConfigValidator.TryValidateActions(
            Config(), Bindings(url), allowPrivateWebhookTargets: false, out var errors);

        Assert.True(ok, string.Join("; ", errors));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Private_targets_pass_when_the_appliance_allows_them()
    {
        var ok = AgentConfigValidator.TryValidateActions(
            Config(), Bindings("http://127.0.0.1:5555/hook"), allowPrivateWebhookTargets: true, out var errors);

        Assert.True(ok, string.Join("; ", errors));
    }

    private static AiAgentConfiguration Config() => new()
    {
        Actions = [new AiAgentToolAction("hook", "test action") { ParametersSampleObject = "{}" }],
    };

    private static Dictionary<string, WebhookBinding> Bindings(string url) =>
        new() { ["hook"] = new WebhookBinding { Url = url } };
}
