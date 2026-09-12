using System.Text.Json;

namespace QuillTests.E2E.Fixtures;

internal static class AgentParameterFixtures
{
    internal static Dictionary<string, JsonElement> Parameters(params (string Name, object? Value)[] entries) =>
        entries.ToDictionary(entry => entry.Name, entry => JsonSerializer.SerializeToElement(entry.Value));
}
