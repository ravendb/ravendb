using System;
using System.Net;
using System.Net.Http;
using Raven.Server.Documents.AI;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.AI;

public class ChatCompletionRetryAfterTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("17ms", 0.017)]
    [InlineData("1m8.754s", 68.754)]
    [InlineData("5m", 300)]
    [InlineData("1h", 3600)]
    [InlineData("250us", 0.00025)]
    [InlineData("42", 42)]
    [InlineData("33011.382867097855", 33011.382867097855)]
    public void TryParseResetTime_parses_the_documented_formats(string input, double expectedSeconds)
    {
        Assert.True(ChatCompletionClient.TryParseResetTime(input, out var time));
        Assert.Equal(expectedSeconds, time.TotalSeconds, precision: 6);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("1684293600")]
    [InlineData("1e300")]
    [InlineData("300000000h")]
    [InlineData("9999999999999m")]
    public void TryParseResetTime_clamps_absurd_values_instead_of_throwing(string input)
    {
        Assert.True(ChatCompletionClient.TryParseResetTime(input, out var time));
        Assert.Equal(ChatCompletionClient.MaxRetryAfter, time);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("")]
    [InlineData("soon")]
    [InlineData("Infinity")]
    [InlineData("NaN")]
    public void TryParseResetTime_rejects_unparseable_values(string input)
    {
        Assert.False(ChatCompletionClient.TryParseResetTime(input, out _));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("-5")]
    [InlineData("-2.5")]
    public void TryParseResetTime_treats_negative_values_as_zero(string input)
    {
        Assert.True(ChatCompletionClient.TryParseResetTime(input, out var time));
        Assert.Equal(TimeSpan.Zero, time);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("Infinity")]
    [InlineData("NaN")]
    [InlineData("-10")]
    [InlineData("0")]
    public void TryGetRetryAfter_ignores_non_positive_or_non_finite_milliseconds(string retryAfterMs)
    {
        using var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
        response.Headers.TryAddWithoutValidation("retry-after-ms", retryAfterMs);

        Assert.False(ChatCompletionClient.TryGetRetryAfter(response, out _));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("1e300")]
    [InlineData("999999999999999")]
    public void TryGetRetryAfter_clamps_absurd_milliseconds(string retryAfterMs)
    {
        using var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
        response.Headers.TryAddWithoutValidation("retry-after-ms", retryAfterMs);

        Assert.True(ChatCompletionClient.TryGetRetryAfter(response, out var retryAfter));
        Assert.Equal(ChatCompletionClient.MaxRetryAfter, retryAfter);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TryGetRetryAfter_reads_milliseconds_before_the_standard_header()
    {
        using var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
        response.Headers.TryAddWithoutValidation("retry-after-ms", "1500");
        response.Headers.TryAddWithoutValidation("Retry-After", "60");

        Assert.True(ChatCompletionClient.TryGetRetryAfter(response, out var retryAfter));
        Assert.Equal(TimeSpan.FromMilliseconds(1500), retryAfter);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TryGetRetryAfter_clamps_a_long_standard_header()
    {
        using var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
        response.Headers.TryAddWithoutValidation("Retry-After", "172800");

        Assert.True(ChatCompletionClient.TryGetRetryAfter(response, out var retryAfter));
        Assert.Equal(ChatCompletionClient.MaxRetryAfter, retryAfter);
    }
}
