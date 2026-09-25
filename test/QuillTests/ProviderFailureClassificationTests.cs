using FastTests;
using Raven.Client.Exceptions;
using Raven.Quill.Agents;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ProviderFailureClassificationTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const string Wrapper = "Raven.Client.Exceptions.AiException: Failed to communicate with the agent 'a', conversation: 'c'.\n ---> ";

    [RavenFact(RavenTestCategory.Quill)]
    public void A_wrapped_provider_failure_is_classified_by_the_inner_type_name_in_the_text()
    {
        Assert.Equal(ProviderFailureKind.RateLimited, Classify("Raven.Client.Exceptions.RateLimitException: Rate limit reached").Kind);
        Assert.Equal(ProviderFailureKind.QuotaExhausted, Classify("Raven.Client.Exceptions.InsufficientQuotaException: out of credits").Kind);
        Assert.Equal(ProviderFailureKind.Refused, Classify("Raven.Client.Exceptions.RefusedToAnswerException: The request was refused by the model: 'no'").Kind);
        Assert.Equal(ProviderFailureKind.Unavailable, Classify("System.Net.Http.HttpRequestException: Connection refused").Kind);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_wrapped_provider_status_is_classified_by_its_status_code()
    {
        var rejected = Classify("Raven.Client.Exceptions.UnsuccessfulAiRequestException: Status Code: Unauthorized, Message: Incorrect API key provided");
        var outage = Classify("Raven.Client.Exceptions.UnsuccessfulAiRequestException: Status Code: InternalServerError, Message: boom");
        var overloaded = Classify("Raven.Client.Exceptions.UnsuccessfulAiRequestException: Status Code: 529, Message: overloaded");

        Assert.Equal(ProviderFailureKind.Credentials, rejected.Kind);
        Assert.False(rejected.Retryable);
        Assert.Equal(ProviderFailureKind.Unavailable, outage.Kind);
        Assert.True(outage.Retryable);
        Assert.Equal(ProviderFailureKind.Unavailable, overloaded.Kind);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_exception_raised_in_process_is_classified_the_same_way()
    {
        Assert.Equal(ProviderFailureKind.Timeout, ProviderFailures.Classify(new ProviderTimeoutException(TimeSpan.FromSeconds(1))).Kind);
        Assert.Equal(ProviderFailureKind.Protocol, ProviderFailures.Classify(new EmptyAnswerException()).Kind);
        Assert.Equal(ProviderFailureKind.Unavailable, ProviderFailures.Classify(new HttpRequestException("down")).Kind);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_unrecognised_message_stays_unknown()
    {
        var failure = ProviderFailures.Classify(new AiException("something else entirely"));

        Assert.Equal(ProviderFailureKind.Unknown, failure.Kind);
        Assert.False(failure.Retryable);
    }

    private static ProviderFailure Classify(string inner) => ProviderFailures.Classify(new AiException(Wrapper + inner + "\n   at x"));
}
