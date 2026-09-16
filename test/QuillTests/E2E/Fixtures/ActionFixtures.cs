using Raven.Quill.Agents;

namespace QuillTests.E2E.Fixtures;

/// The action bindings the agent tests bind.
internal static class ActionFixtures
{
    // url is nullable so the validator tests can build the bindings they expect to be rejected
    public static WebhookBinding Webhook(string? url, string? secret = null, int? maxResponseSize = null) =>
        new() { Url = url, Secret = secret, MaxResponseSize = maxResponseSize };
}
