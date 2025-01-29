using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public abstract class AbstractAiSettings : IDynamicJsonValueConvertible
{
    public abstract bool HasSettings();
    public abstract bool HasCriticalChanges(AbstractAiSettings other);
    public abstract DynamicJsonValue ToJson();
}
