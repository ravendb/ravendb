using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// The configuration for the ONNX model.
/// </summary>
public sealed class EmbeddedSettings : AbstractAiSettings
{
    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other) =>
        other is EmbeddedSettings
            ? AiSettingsCompareDifferences.None
            : AiSettingsCompareDifferences.All;
}
