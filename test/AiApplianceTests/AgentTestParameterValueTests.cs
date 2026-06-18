using System.Text.Json;
using FastTests;
using Raven.AiAppliance.Agents;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

public class AgentTestParameterValueTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Converts_typed_json_parameter_values_for_raven()
    {
        Assert.Equal("42", Convert("\"42\""));
        Assert.Equal(42L, Convert("42"));
        Assert.Equal(true, Convert("true"));
        Assert.Null(Convert("null"));

        var strings = Assert.IsType<DynamicJsonArray>(Convert("[\"one\",\"two\"]"));
        Assert.Equal("one", strings.Items[0]);
        Assert.Equal("two", strings.Items[1]);

        var numbers = Assert.IsType<DynamicJsonArray>(Convert("[1,2.5]"));
        Assert.Equal(1L, numbers.Items[0]);
        Assert.Equal(2.5m, numbers.Items[1]);

        var booleans = Assert.IsType<DynamicJsonArray>(Convert("[true,false]"));
        Assert.Equal(true, booleans.Items[0]);
        Assert.Equal(false, booleans.Items[1]);
    }

    private static object? Convert(string json)
    {
        using var document = JsonDocument.Parse(json);
        return AgentTestParameterValue.Convert(document.RootElement);
    }
}
