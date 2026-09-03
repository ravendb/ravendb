using System.Text.Json;
using FastTests;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AgentParameterValueTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(AiAgentParameterValueType.Number, "42", "42")]
    [InlineData(AiAgentParameterValueType.Number, " 42 ", "42")]
    [InlineData(AiAgentParameterValueType.Number, "-3.5", "-3.5")]
    [InlineData(AiAgentParameterValueType.Boolean, "true", "true")]
    [InlineData(AiAgentParameterValueType.Boolean, "False", "false")]
    [InlineData(AiAgentParameterValueType.String, "users/1", "\"users/1\"")]
    [InlineData(AiAgentParameterValueType.ArrayOfString, "a,b", "[\"a\",\"b\"]")]
    [InlineData(AiAgentParameterValueType.ArrayOfString, " a , b ", "[\"a\",\"b\"]")]
    [InlineData(AiAgentParameterValueType.ArrayOfNumber, "1,2", "[1,2]")]
    [InlineData(AiAgentParameterValueType.ArrayOfBoolean, "true,false", "[true,false]")]
    [InlineData(AiAgentParameterValueType.ArrayOfNumber, "[1,2]", "[1,2]")]
    [InlineData(AiAgentParameterValueType.ArrayOfString, "[\"a\",\"b\"]", "[\"a\",\"b\"]")]
    public void A_string_is_parsed_into_the_declared_type(
        AiAgentParameterValueType type, string supplied, string expected)
    {
        Assert.True(AgentParameterValue.TryNormalize(
            type, AgentParameterValue.FromString(supplied), out var normalized, out var error), error);

        Assert.Equal(expected, normalized.GetRawText());
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(AiAgentParameterValueType.Number, "42", "42")]
    [InlineData(AiAgentParameterValueType.Boolean, "true", "true")]
    [InlineData(AiAgentParameterValueType.String, "\"users/1\"", "\"users/1\"")]
    [InlineData(AiAgentParameterValueType.ArrayOfNumber, "[1,2]", "[1,2]")]
    [InlineData(AiAgentParameterValueType.ArrayOfBoolean, "[true,false]", "[true,false]")]
    [InlineData(AiAgentParameterValueType.ArrayOfString, "[\"a\"]", "[\"a\"]")]
    [InlineData(AiAgentParameterValueType.ArrayOfString, "[]", "[]")]
    public void A_json_value_of_the_declared_type_passes_through(
        AiAgentParameterValueType type, string json, string expected)
    {
        Assert.True(AgentParameterValue.TryNormalize(
            type, Parse(json), out var normalized, out var error), error);

        Assert.Equal(expected, normalized.GetRawText());
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(AiAgentParameterValueType.Number, "\"abc\"")]
    [InlineData(AiAgentParameterValueType.Number, "true")]
    [InlineData(AiAgentParameterValueType.Number, "[1]")]
    [InlineData(AiAgentParameterValueType.Boolean, "\"yes\"")]
    [InlineData(AiAgentParameterValueType.Boolean, "1")]
    [InlineData(AiAgentParameterValueType.String, "[\"a\"]")]
    [InlineData(AiAgentParameterValueType.ArrayOfNumber, "\"a,b\"")]
    [InlineData(AiAgentParameterValueType.ArrayOfNumber, "[1,\"a\"]")]
    [InlineData(AiAgentParameterValueType.ArrayOfBoolean, "\"true,maybe\"")]
    [InlineData(AiAgentParameterValueType.ArrayOfString, "42")]
    public void A_value_that_cannot_be_the_declared_type_is_rejected(
        AiAgentParameterValueType type, string json)
    {
        Assert.False(AgentParameterValue.TryNormalize(type, Parse(json), out _, out var error));
        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("1e400")]
    [InlineData("-1e400")]
    [InlineData("NaN")]
    [InlineData("Infinity")]
    [InlineData("+48123456789")]
    public void A_number_json_cannot_carry_is_rejected_instead_of_throwing(string supplied)
    {
        Assert.False(AgentParameterValue.TryNormalize(
            AiAgentParameterValueType.Number, AgentParameterValue.FromString(supplied), out _, out var error));

        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_json_number_past_double_is_rejected_instead_of_throwing()
    {
        Assert.False(AgentParameterValue.TryNormalize(
            AiAgentParameterValueType.Number, Parse("1e400"), out _, out var error));

        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("1e-40", "1E-40")]
    [InlineData("1e30", "1E+30")]
    [InlineData("0.1", "0.1")]
    public void A_number_below_decimals_range_keeps_its_magnitude(string supplied, string expected)
    {
        Assert.True(AgentParameterValue.TryNormalize(
            AiAgentParameterValueType.Number, AgentParameterValue.FromString(supplied),
            out var normalized, out var error), error);

        Assert.Equal(expected, normalized.GetRawText());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void The_default_type_carries_any_json_through_unchanged()
    {
        foreach (var json in new[] { "42", "\"users/1\"", "true", "[1,\"a\"]", "null" })
        {
            Assert.True(AgentParameterValue.TryNormalize(
                AiAgentParameterValueType.Default, Parse(json), out var normalized, out _));

            Assert.Equal(json, normalized.GetRawText());
        }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Stored_text_round_trips_and_a_legacy_raw_value_reads_back_as_a_string()
    {
        Assert.Equal("[1,2]", AgentParameterValue.ToStoredText(AgentParameterValue.FromStoredText("[1,2]")));
        Assert.Equal("users/1", AgentParameterValue.FromStoredText("users/1").GetString());
        Assert.Equal("users/1", AgentParameterValue.FromStoredText("\"users/1\"").GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Display_text_unwraps_a_string_and_leaves_other_json_as_written()
    {
        Assert.Equal("users/1", AgentParameterValue.ToDisplayText(Parse("\"users/1\"")));
        Assert.Equal("42", AgentParameterValue.ToDisplayText(Parse("42")));
        Assert.Equal("[1,2]", AgentParameterValue.ToDisplayText(Parse("[1,2]")));
    }

    private static JsonElement Parse(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }
}
