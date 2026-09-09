using FastTests;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class WizardErrorFormatterTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Quill)]
    // ex.ToString() from the server: type prefix + stack trace both stripped.
    [InlineData(
        "System.ArgumentException: Format of the initialization string does not conform to specification starting at index 0.\r\n   at System.Data.Common.DbConnectionOptions.GetKeyValuePair()\r\n   at Foo.Bar()",
        "Format of the initialization string does not conform to specification starting at index 0.")]
    // HRESULT in the type prefix is stripped too.
    [InlineData(
        "Npgsql.NpgsqlException (0x80004005): Failed to connect to 127.0.0.1:5432\n   at Npgsql.Foo()",
        "Failed to connect to 127.0.0.1:5432")]
    // Nested "---> Inner" markers collapse; inner type prefix goes as well.
    [InlineData(
        "System.Exception: outer ---> System.Net.Sockets.SocketException: No such host is known.",
        "outer No such host is known.")]
    // The verifier's plain-language lead is preserved; only the appended exception is cleaned.
    [InlineData(
        "Could not connect to source database: System.ArgumentException: bad string\r\n   at X()",
        "Could not connect to source database: bad string")]
    public void Sanitize_strips_exception_noise(string raw, string expected)
    {
        Assert.Equal(expected, WizardErrorFormatter.Sanitize(raw));
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Sanitize_returns_placeholder_for_empty_input(string? raw)
    {
        Assert.Equal("Unknown error.", WizardErrorFormatter.Sanitize(raw));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void FormatConnectionError_summary_is_friendly_and_details_keep_the_stack_trace()
    {
        const string raw =
            "Npgsql.NpgsqlException (0x80004005): 28P01: password authentication failed for user \"quill\"\n   at Npgsql.Foo()";

        var formatted = WizardErrorFormatter.FormatConnectionError(raw);

        // Summary: actionable hint + provider reason, no exception-type noise or stack trace.
        Assert.StartsWith("Could not connect to the source database.", formatted.Message);
        Assert.Contains("password authentication failed for user \"quill\"", formatted.Message);
        Assert.DoesNotContain("Exception", formatted.Message);
        Assert.DoesNotContain("   at ", formatted.Message);

        // Details: the untouched raw text, so the stack trace is still available behind a disclosure.
        Assert.Equal(raw, formatted.Details);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void FormatConnectionError_omits_details_when_there_is_nothing_beyond_the_summary()
    {
        var formatted = WizardErrorFormatter.FormatConnectionError("Login timeout expired");

        Assert.Contains("Login timeout expired", formatted.Message);
        Assert.Null(formatted.Details);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Format_keeps_verifier_lead_in_summary_and_stack_trace_in_details()
    {
        const string raw = "Could not connect to source database: System.ArgumentException: bad string\r\n   at X()";

        var formatted = WizardErrorFormatter.Format(raw);

        Assert.Equal("Could not connect to source database: bad string", formatted.Message);
        Assert.Equal(raw.Trim(), formatted.Details);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Format_omits_details_for_a_clean_single_line_message()
    {
        // A verifier message with no exception type or stack trace has nothing extra to disclose.
        var formatted = WizardErrorFormatter.Format(
            "PostgreSQL wal_level is 'replica', but must be 'logical' for CDC.");

        Assert.Equal("PostgreSQL wal_level is 'replica', but must be 'logical' for CDC.", formatted.Message);
        Assert.Null(formatted.Details);
    }
}
