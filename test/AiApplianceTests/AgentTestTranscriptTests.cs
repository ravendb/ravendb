using System.Text.Json;
using FastTests;
using Raven.AiAppliance.Agents;
using Raven.Client.Documents.Operations.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Fast unit tests for reconstructing the query tool-call transcript from a draft agent test
/// run's result object (the data the wizard's "Test agent" panel renders).
/// </summary>
public class AgentTestTranscriptTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static AiAgentConfiguration ConfigWithSearchProducts() => new()
    {
        Queries =
        [
            new AiAgentToolQuery
            {
                Name = "search-products",
                Description = "Search products by name.",
                Query = "from Products where search(Name, $searchTerm)",
            },
        ],
    };

    private static IReadOnlyList<AgentQueryToolCall> Extract(string json, AiAgentConfiguration configuration)
    {
        using var doc = JsonDocument.Parse(json);
        return AgentTestTranscript.ExtractQueryToolCalls(doc.RootElement, configuration);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Pairs_assistant_call_with_its_tool_result_and_query_config()
    {
        const string json = """
        {
            "ConversationId": "TestConversation",
            "Response": { "reply": "Found one." },
            "Documents": {
                "TestConversation": {
                    "Agent": "agents/sales",
                    "Messages": [
                        { "role": "system", "content": "You help shoppers." },
                        { "role": "assistant", "tool_calls": [
                            { "id": "call_1", "type": "function", "function": { "name": "search-products", "arguments": "{\"searchTerm\":\"mouse\"}" } }
                        ] },
                        { "role": "tool", "tool_call_id": "call_1", "content": "[{\"Name\":\"Wireless Mouse\"}]" },
                        { "role": "assistant", "content": "{\"reply\":\"Found one.\"}" }
                    ]
                }
            }
        }
        """;

        var toolCalls = Extract(json, ConfigWithSearchProducts());

        var call = Assert.Single(toolCalls);
        Assert.Equal("call_1", call.Id);
        Assert.Equal("search-products", call.Name);
        Assert.Equal("Search products by name.", call.Description);
        Assert.Equal("from Products where search(Name, $searchTerm)", call.Query);
        Assert.Equal("{\"searchTerm\":\"mouse\"}", call.Arguments);
        Assert.Equal("[{\"Name\":\"Wireless Mouse\"}]", call.Result);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Keeps_the_call_even_when_no_result_message_is_present()
    {
        const string json = """
        {
            "Documents": {
                "TestConversation": {
                    "Messages": [
                        { "role": "assistant", "tool_calls": [
                            { "id": "call_1", "type": "function", "function": { "name": "search-products", "arguments": "{}" } }
                        ] }
                    ]
                }
            }
        }
        """;

        var call = Assert.Single(Extract(json, ConfigWithSearchProducts()));
        Assert.Equal("search-products", call.Name);
        Assert.Equal("{}", call.Arguments);
        Assert.Null(call.Result);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Skips_calls_that_do_not_match_a_configured_query_tool()
    {
        // The model invoked some other tool name (e.g. an action) — the appliance only surfaces
        // query tools, so it is dropped.
        const string json = """
        {
            "Documents": {
                "TestConversation": {
                    "Messages": [
                        { "role": "assistant", "tool_calls": [
                            { "id": "call_1", "type": "function", "function": { "name": "escalate-to-human", "arguments": "{}" } }
                        ] },
                        { "role": "tool", "tool_call_id": "call_1", "content": "ok" }
                    ]
                }
            }
        }
        """;

        Assert.Empty(Extract(json, ConfigWithSearchProducts()));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Skips_tool_calls_that_are_not_on_an_assistant_message()
    {
        // Only assistant messages carry the model's tool invocations; tool_calls riding on any
        // other role (here a "tool" message) are not the model's calls and must be ignored.
        const string json = """
        {
            "Documents": {
                "TestConversation": {
                    "Messages": [
                        { "role": "tool", "tool_call_id": "call_1", "content": "[]", "tool_calls": [
                            { "id": "call_1", "type": "function", "function": { "name": "search-products", "arguments": "{}" } }
                        ] }
                    ]
                }
            }
        }
        """;

        Assert.Empty(Extract(json, ConfigWithSearchProducts()));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Returns_empty_when_the_result_has_no_documents()
    {
        const string json = """{ "ConversationId": "TestConversation", "Response": { "reply": "Hi." } }""";
        Assert.Empty(Extract(json, ConfigWithSearchProducts()));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Returns_empty_when_the_configuration_declares_no_query_tools()
    {
        const string json = """
        {
            "Documents": {
                "TestConversation": {
                    "Messages": [
                        { "role": "assistant", "tool_calls": [
                            { "id": "call_1", "type": "function", "function": { "name": "search-products", "arguments": "{}" } }
                        ] }
                    ]
                }
            }
        }
        """;

        Assert.Empty(Extract(json, new AiAgentConfiguration()));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Extracts_multiple_calls_in_conversation_order()
    {
        const string json = """
        {
            "Documents": {
                "TestConversation": {
                    "Messages": [
                        { "role": "assistant", "tool_calls": [
                            { "id": "call_1", "type": "function", "function": { "name": "search-products", "arguments": "{\"searchTerm\":\"mouse\"}" } }
                        ] },
                        { "role": "tool", "tool_call_id": "call_1", "content": "[1]" },
                        { "role": "assistant", "tool_calls": [
                            { "id": "call_2", "type": "function", "function": { "name": "search-products", "arguments": "{\"searchTerm\":\"hub\"}" } }
                        ] },
                        { "role": "tool", "tool_call_id": "call_2", "content": "[2]" }
                    ]
                }
            }
        }
        """;

        var toolCalls = Extract(json, ConfigWithSearchProducts());

        Assert.Equal(2, toolCalls.Count);
        Assert.Equal("call_1", toolCalls[0].Id);
        Assert.Equal("[1]", toolCalls[0].Result);
        Assert.Equal("call_2", toolCalls[1].Id);
        Assert.Equal("[2]", toolCalls[1].Result);
    }
}
