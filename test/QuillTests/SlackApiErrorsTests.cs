using FastTests;
using Raven.Quill.Slack;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class SlackApiErrorsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void An_http_client_timeout_is_a_transport_failure_not_a_cancellation()
    {
        var timeout = new TaskCanceledException("timed out", new TimeoutException());

        Assert.True(SlackApiErrors.IsTransportFailure(timeout));
        Assert.Equal("the Slack API did not respond in time", SlackApiErrors.Describe(timeout));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_caller_cancellation_is_not_a_transport_failure()
    {
        var cancelled = new TaskCanceledException();

        Assert.False(SlackApiErrors.IsTransportFailure(cancelled));
        Assert.False(SlackApiErrors.IsApiFailure(cancelled));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Only_failures_raised_by_the_slack_client_count_as_api_failures()
    {
        var wrapped = new SlackApiUnavailableException("could not reach the Slack API: refused", new HttpRequestException("refused"));

        Assert.True(SlackApiErrors.IsApiFailure(wrapped));
        Assert.Equal("could not reach the Slack API: refused", SlackApiErrors.Describe(wrapped));
        Assert.False(SlackApiErrors.IsApiFailure(new HttpRequestException("the agent's RavenDB is down")));
        Assert.False(SlackApiErrors.IsApiFailure(new NullReferenceException()));
    }
}
