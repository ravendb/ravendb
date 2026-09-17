using System.Collections.Generic;

namespace InterversionTests.IndexDefinitionCompatibility;

internal sealed class GeneratorOutput
{
    public string CompilerFingerprint { get; init; }
    public string RavenClientProductVersion { get; init; }
    public Dictionary<string, DefinitionText> Definitions { get; init; }
}

internal sealed class DefinitionText
{
    public string[] Maps { get; init; }
    public string Reduce { get; init; }
}
