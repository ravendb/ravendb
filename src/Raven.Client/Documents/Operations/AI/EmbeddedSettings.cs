using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// The configuration for the ONNX model.
/// </summary>
public sealed class EmbeddedSettings : AbstractAiSettings
{
    // We're using a server-wide, singleton ONNX service, and it can't be configured intentionally.
    public override void ValidateFields(List<string> errors)
    {
        // nothing to validate
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other) =>
        other is EmbeddedSettings
            ? AiSettingsCompareDifferences.None
            : AiSettingsCompareDifferences.All;

    public override DynamicJsonValue ToJson() => new();
}
