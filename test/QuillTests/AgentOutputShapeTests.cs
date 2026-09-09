using FastTests;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AgentOutputShapeTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void ResolveReplyField_takes_first_sample_object_property()
    {
        var config = new AiAgentConfiguration { SampleObject = """{"reply":""}""" };
        Assert.Equal("reply", AgentOutputShape.ResolveReplyField(config));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ResolveReplyField_preserves_first_property_casing_and_order()
    {
        var config = new AiAgentConfiguration { SampleObject = """{"Reply":"x","Related":[]}""" };
        Assert.Equal("Reply", AgentOutputShape.ResolveReplyField(config));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ResolveReplyField_falls_back_to_schema_properties_when_sample_absent()
    {
        var config = new AiAgentConfiguration
        {
            SampleObject = null,
            OutputSchema = """{"type":"object","properties":{"Answer":{"type":"string"},"Score":{"type":"number"}}}""",
        };
        Assert.Equal("Answer", AgentOutputShape.ResolveReplyField(config));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ResolveReplyField_defaults_to_reply_when_sample_is_malformed_and_no_schema()
    {
        var config = new AiAgentConfiguration { SampleObject = "{not valid json" };
        Assert.Equal(AgentOutputShape.DefaultReplyField, AgentOutputShape.ResolveReplyField(config));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ResolveReplyField_defaults_to_reply_when_nothing_provided()
    {
        Assert.Equal("reply", AgentOutputShape.ResolveReplyField(new AiAgentConfiguration()));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ResolveReplyField_ignores_non_object_sample()
    {
        var config = new AiAgentConfiguration { SampleObject = "[1,2,3]" };
        Assert.Equal("reply", AgentOutputShape.ResolveReplyField(config));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ResolveReplyField_defaults_to_reply_for_a_missing_agent()
    {
        Assert.Equal(AgentOutputShape.DefaultReplyField, AgentOutputShape.ResolveReplyField(null));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ExtractReplyText_matches_field_case_insensitively()
    {
        var answer = new Dictionary<string, object> { ["Reply"] = "hello" };
        Assert.Equal("hello", AgentOutputShape.ExtractReplyText(answer, "reply"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ExtractReplyText_returns_empty_when_field_missing()
    {
        var answer = new Dictionary<string, object> { ["Other"] = "x" };
        Assert.Equal("", AgentOutputShape.ExtractReplyText(answer, "reply"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ExtractReplyText_returns_empty_for_null_answer()
    {
        Assert.Equal("", AgentOutputShape.ExtractReplyText(null, "reply"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ExtractReplyText_returns_empty_for_null_value()
    {
        var answer = new Dictionary<string, object> { ["reply"] = null! };
        Assert.Equal("", AgentOutputShape.ExtractReplyText(answer, "reply"));
    }
}
