using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public interface IDynamicJsonValueConvertible
    {
        DynamicJsonValue ToJson();
    }
}
