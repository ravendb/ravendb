using Sparrow.Json.Parsing;

namespace Raven.Server.EventListener;

public abstract class Event : IDynamicJson
{
    public EventType Type { get; }

    protected Event(EventType type)
    {
        Type = type;
    }

    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Type)] = Type
        };
    }
}
