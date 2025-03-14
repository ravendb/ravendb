using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public abstract class AbstractAiSettings : IDynamicJsonValueConvertible
{
    public abstract void ValidateFields(List<string> errors);

    public abstract AiSettingsCompareDifferences Compare(AbstractAiSettings other);

    public abstract DynamicJsonValue ToJson();
}
