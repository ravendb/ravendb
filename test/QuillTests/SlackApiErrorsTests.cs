using FastTests;
using Raven.Quill.Slack;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class SlackApiErrorsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void An_http_client_timeout_is_an_api_failure_not_a_cancellation()
    {
        var timeout = new TaskCanceledException("timed out", new TimeoutException());

        Assert.True(SlackApiErrors.IsApiFailure(timeout));
        Assert.Equal("the Slack API did not respond in time", SlackApiErrors.Describe(timeout));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_caller_cancellation_is_not_an_api_failure()
    {
        var cancelled = new TaskCanceledException();

        Assert.False(SlackApiErrors.IsApiFailure(cancelled));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_unrelated_bug_is_not_an_api_failure()
    {
        Assert.False(SlackApiErrors.IsApiFailure(new NullReferenceException()));
    }
}
